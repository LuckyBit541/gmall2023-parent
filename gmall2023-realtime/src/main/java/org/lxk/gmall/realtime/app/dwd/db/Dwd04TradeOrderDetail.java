package org.lxk.gmall.realtime.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.SQlUtil;

import java.time.Duration;

public class Dwd04TradeOrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new Dwd04TradeOrderDetail().start(30003, 2, "Dwd04TradeOrderDetail");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // 1 read osd_db
        readOdsDb(tEnv, "Dwd04TradeOrderDetail");
        // 2 get oderInfo
        Table oder_info = tEnv.sqlQuery(
                "select" +
                        " data['id'] id," +
                        " data['user_id'] user_id ," +
                        " data['province_id'] province_id " +
                        " from " +
                        "ods_db " +
                        "where " +
                        " `type` = 'insert' " +
                        "and  `database` = 'gmall2023'  " +
                        "and  `table` = 'order_info'  "
        );
        tEnv.createTemporaryView("order_info", oder_info);
        // 3 get oder_detail
        Table order_detail = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['sku_name'] sku_name," +
                        "data['img_url'] img_url," +
                        "data['order_price'] order_price," +
                        "data['create_time'] create_time," +
                        "data['source_type'] source_type," +
                        "data['source_id'] source_id," +
                        "data['sku_num'] sku_num," +
                        "cast(cast(data['sku_num'] as decimal(16,2) ) * cast(data['order_price'] as decimal(16,2) ) as string) split_original_amount ," +
                        "data['split_total_amount'] split_total_amount," +
                        "data['split_activity_amount'] split_activity_amount," +
                        "data['split_coupon_amount'] split_coupon_amount," +
                        "data['operate_tim'] operate_time," +
                        "ts" +
                        " from " +
                        "ods_db " +
                        "where " +
                        " `type` = 'insert' " +
                        "and  `database` = 'gmall2023'  " +
                        "and  `table` = 'order_detail'  "
        );
        tEnv.createTemporaryView("order_detail", order_detail);

        // 4 get order_detail_activity
        Table order_detail_activity = tEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id," +
                        "data['activity_id'] activity_id," +
                        "data['activity_rule_id'] activity_rule_id " +
                        " from " +
                        "ods_db " +
                        "where " +
                        "`type` = 'insert' " +
                        "and  `database` = 'gmall2023'  " +
                        "and  `table` = 'order_detail_activity'  "

        );
        tEnv.createTemporaryView("order_detail_activity", order_detail_activity);
        // 5 get coupon_use
        Table order_detail_coupon = tEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id," +
                        "data['coupon_id'] coupon_id " +
                        " from " +
                        "ods_db " +
                        "where " +
                        "`type` = 'insert' " +
                        "and  `database` = 'gmall2023'  " +
                        "and  `table` = 'order_detail_coupon'  "
        );
        tEnv.createTemporaryView("order_detail_coupon",order_detail_coupon);
        // 6 join
        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id, " +
                        "od.order_id, " +
                        "oi.user_id, " +
                        "od.sku_id, " +
                        "od.sku_name, " +
                        "oi.province_id, " +
                        "act.activity_id, " +
                        "act.activity_rule_id, " +
                        "cou.coupon_id, " +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id, " +
                        "od.create_time, " +
                        "od.sku_num, " +
                        "od.split_original_amount, " +
                        "od.split_activity_amount, " +
                        "od.split_coupon_amount, " +
                        "od.split_total_amount, " +
                        "od.ts  " +
                        "from " +
                        "order_detail od " +
                        "inner join order_info oi " +
                        "on od.order_id = oi.id " +
                        " left join order_detail_activity act " +
                        " on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id= cou.order_detail_id"
        );
        // 7 create trade_oder_detail
        tEnv.executeSql(
                "create table dwd_trade_order_detail( " +
                        "id string, " +
                        "order_id string, " +
                        "user_id string, " +
                        "sku_id string, " +
                        "sku_name string, " +
                        "province_id string, " +
                        "activity_id string, " +
                        "activity_rule_id string, " +
                        "coupon_id string, " +
                        "date_id string, " +
                        "create_time string, " +
                        "sku_num string, " +
                        "split_original_amount string, " +
                        "split_activity_amount string, " +
                        "split_coupon_amount string, " +
                        "split_total_amount string, " +
                        "ts bigint, " +
                        "primary key(id) not enforced " +
                        ")" + SQlUtil.getUpsertKafka( GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );
        // 8 insert data
        result.executeInsert("dwd_trade_order_detail");
        //tEnv.sqlQuery("select * from order_detail_coupon").execute().print();


    }
}
