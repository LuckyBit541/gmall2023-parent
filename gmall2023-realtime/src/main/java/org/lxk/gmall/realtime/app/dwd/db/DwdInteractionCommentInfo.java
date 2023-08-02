package org.lxk.gmall.realtime.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.SQlUtil;

public class DwdInteractionCommentInfo extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(30002, 2, "DwdInteractionCommentInfo");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1 read data from ods_db topic of Kafka
        readOdsDb(tEnv, "DwdInteractionCommentInfo");
        //tEnv.sqlQuery("select * from ods_db").execute().print();
        // 2 filter comment data out of raw data

        Table comment_info = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['sku_id'] sku_id," +
                        "data['appraise'] appraise," +
                        "data['comment_txt'] comment_txt," +
                        "ts ," +
                        "pt " +
                        "from ods_db " +
                        "where `database`= 'gmall2023' " +
                        "and `table` = 'comment_info' " +
                        "and `type`='insert'"
        );
        tEnv.createTemporaryView("comment_info", comment_info);
        // 3 load dic data from Hbase


        readBaseDic(tEnv, "DwdInteractionCommentInfo");

        // 4 join comment data with dic data
        Table result = tEnv.sqlQuery(
                "select ci.id ,user_id, sku_id,ci.appraise, dic.info.dic_name appraise_name," +
                        "comment_txt ," +
                        "ts " +
                        "from " +
                        "comment_info ci " +
                        "left join base_dic for system_time as of ci.pt as dic " +
                        "on ci.appraise = dic.dic_code"
        );

        tEnv.sqlQuery("select * from base_dic").execute().print();
        tEnv.createTemporaryView("result", result);
        tEnv.executeSql("create table dwd_interaction_comment_info(" +
                "id string primary key not enforced," +
                "user_id string," +
                "sku_id string," +
                "appraise string," +
                "appraise_name string," +
                "comment_txt string," +
                "ts bigint " +
                ")" + SQlUtil.getUpsertKafka(GmallConstant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 5 write processed data down into Kafka
        result.executeInsert("dwd_interaction_comment_info");


    }
}
