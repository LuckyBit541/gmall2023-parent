package org.lxk.gmall.realtime.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.SQlUtil;

import java.time.Duration;

public class Dwd05TradeCanceledOrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new Dwd05TradeCanceledOrderDetail().start(30003, 2, "Dwd04TradeOrderDetail");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        // 1 read osd_db
        readOdsDb(tEnv, "Dwd04TradeOrderDetail");
        // 2 get cancel_order_info
        Table cancel_order_info = tEnv.sqlQuery(
                "select" +
                        " data['id'] id ," +
                        "data['operate_time'] cancel_time ," +
                        "ts" +
                        " from " +
                        "ods_db " +
                        "where " +
                        " `type` = 'update' " +
                        "and  `database` = 'gmall2023'  " +
                        "and  `table` = 'order_info' " +
                        "and `old`['order_status']='1001' " +
                        "and data['order_status']='1003' "
        );
        tEnv.createTemporaryView("cancel_order_info", cancel_order_info);
        // 3 load order_detail
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
                        "ts bigint " +

                        ")" + SQlUtil.getKafkaSourceSql("Dwd05TradeCanceledOrderDetail",GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );
        // 4 join

        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id, " +
                        "od.order_id, " +
                        "od.user_id, " +
                        "od.sku_id, " +
                        "od.sku_name, " +
                        "od.province_id, " +
                        "od.activity_id, " +
                        "od.activity_rule_id, " +
                        "od.coupon_id, " +
                        "date_format(coi.cancel_time, 'yyyy-MM-dd') date_id, " +
                        "od.sku_num, " +
                        "coi.cancel_time cancel_time ," +
                        "od.split_original_amount, " +
                        "od.split_activity_amount, " +
                        "od.split_coupon_amount, " +
                        "od.split_total_amount, " +
                        "coi.ts  " +
                        "from " +
                        " cancel_order_info coi " +
                        " join dwd_trade_order_detail od " +
                        "on coi.id =od.order_id "
        );
        // create cancel_order_detail
        tEnv.executeSql(
                "create table cancel_order_detail( " +
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
                        "sku_num string, " +
                        "cancel_time string," +
                        "split_original_amount string, " +
                        "split_activity_amount string, " +
                        "split_coupon_amount string, " +
                        "split_total_amount string, " +
                        "ts bigint " +
                        ")" + SQlUtil.getKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_CANCEL_ORDER_DETAIL)
        );

        result.executeInsert("cancel_order_detail");
       // tEnv.sqlQuery("select * from cancel_order_info").execute().print();


    }
}
