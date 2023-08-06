package org.lxk.gmall.realtime.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.SQlUtil;

public class Dwd08TradeRefundPaySuc extends BaseSqlApp {

    public static void main(String[] args) {
        new Dwd08TradeRefundPaySuc().start(30008, 2, "Dwd08TradeRefundPaySuc");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1 load data
        readOdsDb(tEnv, "Dwd08TradeRefundPaySuc");
        // 2 filter successful refund data in refund_info
        Table refundPayment = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['payment_type'] payment_type," +
                        "data['callback_time'] callback_time," +
                        "data['total_amount'] total_amount," +
                        "pt," +
                        "ts " +
                        "from ods_db " +
                        "where `database`='gmall2023' " +
                        "and `table`='refund_payment' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status'] = '1602' ");
        tEnv.createTemporaryView("refund_payment", refundPayment);
        // 3 filter refund dada in order_refund_info
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_num'] refund_num," +
                        "`old` " +
                        "from ods_db " +
                        "where `database`='gmall2023' " +
                        "and `table`='order_refund_info' " +
                        "and `type`='update' " +
                        "and `old`['refund_status'] is not null " +
                        "and `data`['refund_status'] = '0705' "
        );
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);
        // 4 filter data from order_info
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id," +
                        "`old` " +
                        "from ods_db " +
                        "where `database`='gmall2023' " +
                        "and `table`='order_info' " +
                        "and `type`='update' " +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status'] = '1006' ");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 5 load base_dic
        readBaseDic(tEnv, "Dwd08TradeRefundPaySuc");
        tEnv.sqlQuery("select * from base_dic").execute().print();
        // 6 join
        Table result = tEnv.sqlQuery(
                "select " +
                        "rp.id," +
                        "oi.user_id," +
                        "rp.order_id," +
                        "rp.sku_id," +
                        "oi.province_id," +
                        "rp.payment_type," +
                        "dic.info.dic_name payment_type_name," +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id," +
                        "rp.callback_time," +
                        "ri.refund_num," +
                        "rp.total_amount," +
                        "rp.ts " +
                        "from refund_payment rp " +
                        "join order_refund_info ri " +
                        "on rp.order_id=ri.order_id and rp.sku_id=ri.sku_id " +
                        "join order_info oi " +
                        "on rp.order_id=oi.id " +
                        "join base_dic for system_time as of rp.pt as dic " +
                        "on rp.payment_type=dic.dic_code ");

        // 7 save data
        tEnv.executeSql("create table dwd_trade_refund_pay_suc(" +
                "id string," +
                "user_id string," +
                "order_id string," +
                "sku_id string," +
                "province_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "date_id string," +
                "callback_time string," +
                "refund_num string," +
                "refund_amount string," +
                "ts bigint " +
                ")" + SQlUtil.getKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_REFUND_PAY_SUC));
        result.executeInsert("dwd_trade_refund_pay_suc");


    }
}
