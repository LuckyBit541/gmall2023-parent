package org.lxk.gmall.realtime.app.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.Kasplit;
import org.lxk.gmall.realtime.util.SQlUtil;

public class Dws_01_DwsTrafficSourceKeywordPageView extends BaseSqlApp {

    public static void main(String[] args) {
        new Dws_01_DwsTrafficSourceKeywordPageView().start(40001, 2, "Dws_01_DwsTrafficSourceKeywordPageView");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1 load data from kafka
        tEnv.executeSql(
                "create table page_log( " +
                        "page row<last_page_id string, item_type string, item string>, " +
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts,3)," +
                        "watermark for et as et - interval '3' second " +
                        ")" + SQlUtil.getKafkaSourceSql("Dws_01_DwsTrafficSourceKeywordPageView", GmallConstant.TOPIC_DWD_TRAFFIC_PAGE)
        );
        // 2 filter data
        Table kw_table = tEnv.sqlQuery(
                "select " +
                        "page.item keywords," +
                        "et " +
                        "from page_log " +
                        " where( page.last_page_id='home' or " +
                        "  page.last_page_id='search' )" +
                        "and page.item_type='keyword' " +
                        "and page.item is not null"
        );
        tEnv.createTemporaryView("kw_table", kw_table);

        // 3.1 table function
        tEnv.createTemporaryFunction("kw_split", Kasplit.class);
        // 3.2 deal with keywords
        Table split_table = tEnv.sqlQuery(
                "select " +
                        "kw," +
                        "et " +
                        "from kw_table " +
                        "join lateral table(kw_split(keywords)) on true"
        );
        tEnv.createTemporaryView("split_table", split_table);

        //tEnv.sqlQuery("select * from split_table").execute().print();

        // 4 tvf aggregate
        Table result = tEnv.sqlQuery(
                "select " +
                        "date_format(window_start,'yyyy-MM-dd HH:mm:ss') start_time," +
                        "date_format(window_end,'yyyy-MM-dd HH:mm:ss') end_time," +
                        "kw ," +
                        "date_format(window_start,'yyyyMMdd') cur_date," +
                        "count(*) ct " +
                        "from table(tumble(table split_table,descriptor(et),interval '5' second)) " +
                        "group by window_start,window_end ,kw"
        );
       // result.execute().print();

        // save data to doris
        tEnv.executeSql(
                "create table result_table(" +
                        "stt string," +
                        "edt string," +
                        "keyword string," +
                        "cur_date string," +
                        " keyword_count bigint " +

                        ")" + SQlUtil.getDorisSinkSql("gmall2023.dws_traffic_source_keyword_page_view_window")
        );
        result.executeInsert("result_table");


    }
}
