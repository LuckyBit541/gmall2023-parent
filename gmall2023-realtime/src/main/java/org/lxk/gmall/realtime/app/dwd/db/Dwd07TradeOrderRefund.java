package org.lxk.gmall.realtime.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.SQlUtil;

public class Dwd07TradeOrderRefund extends BaseSqlApp {

    public static void main(String[] args) {
        new Dwd07TradeOrderRefund().start(30007, 2, "Dwd07TradeOrderRefund");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1 load data
        readOdsDb(tEnv, "Dwd07TradeOrderRefund");
        // 2 filter refund data in refund_info
        Table refundInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_type'] refund_type," +
                        "data['refund_num'] refund_num," +
                        "data['refund_amount'] refund_amount," +
                        "data['refund_reason_type'] refund_reason_type," +
                        "data['refund_reason_txt'] refund_reason_txt," +
                        "data['operate_time'] operate_time, " +
                        "pt , " +
                        "ts " +
                        " from " +
                        "ods_db " +
                        "where " +
                        " `type` = 'update' " +
                        "and  `database` = 'gmall2023'  " +
                        "and  `table` = 'order_refund_info' " +
                        "and `old`['refund_status'] is not null " +
                        "and data['refund_status']='0702' "
        );
        tEnv.createTemporaryView("refund_info", refundInfo);
        // 3 filter refund dada in order_info
        Table orderInfo = tEnv.sqlQuery(
                "select  " +
                        "data['id'] id," +
                        "data['province_id'] province_id " +
                        "from " +
                        "ods_db " +
                        "where " +
                        " `type` = 'update' " +
                        "and  `database` = 'gmall2023'  " +
                        "and  `table` = 'order_info' " +
                        "and `old`['order_status'] is not null " +
                        "and data['order_status']='1005' "
        );
        tEnv.createTemporaryView("order_info", orderInfo);

        // 4 load base_dic
        readBaseDic(tEnv, "Dwd07TradeOrderRefund1");
        // 5 join
        Table result = tEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.operate_time,'yyyy-MM-dd') date_id," +
                        "ri.operate_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name refund_type_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name refund_reason_type_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from " +
                        "refund_info ri join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code  " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code  "
        );

        // 6 save data
        tEnv.executeSql(

                "create table dwd_trade_order_refund (" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "operate_time string," +
                        "refund_type string," +
                        "refund_type_name string," +
                        "refund_reason_type string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts bigint" +
                        ")" + SQlUtil.getKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND)
        );
        result.executeInsert("dwd_trade_order_refund");


    }
}
