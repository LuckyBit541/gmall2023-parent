package org.lxk.gmall.realtime.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.SQlUtil;

import java.time.Duration;

public class Dwd06TradePayDetailSuc extends BaseSqlApp {
    public static void main(String[] args) {
        new Dwd06TradePayDetailSuc().start(30003, 2, "Dwd04TradeOrderDetail");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        //tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 + 5));
        // 1 read osd_db
        readOdsDb(tEnv, "Dwd06TradePayDetailSuc1");
        // 2 filter successful payment
        Table successful_payment_order = tEnv.sqlQuery(
                "select " +
                        "data['order_id'] order_id," +
                        "data['user_id'] user_id," +
                        "data['payment_type'] payment_type," +
                        "data['callback_time'] callback_time ,"+
                        "ts ," +
                        "et ," +
                        "pt " +
                        " from " +
                        "ods_db " +
                        "where " +
                        " `type` = 'update' " +
                        "and  `database` = 'gmall2023'  " +
                        "and  `table` = 'payment_info' " +
                        "and `old`['payment_status']='1601' " +
                        "and data['payment_status']='1602' "
        );
        tEnv.createTemporaryView("successful_payment_order", successful_payment_order);
        // 3 load  dwd_order_detail
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
                        "ts bigint ," +
                        "et as to_timestamp_ltz(ts,0)," +
                        "watermark for et as et - interval '3' second " +
                        ")" + SQlUtil.getKafkaSourceSql("Dwd06TradePayDetailSuc",GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL)
        );
        // 4 load base_dic
        readBaseDic(tEnv,"Dwd06TradePayDetailSuc");
        // 5 join: interval join    lookup join: 退化支付类型
        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code," +
                        "dic.info.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from " +
                        "successful_payment_order pi " +
                        "join dwd_trade_order_detail od " +
                        "on  pi.order_id = od.order_id " +
                        "and od.et >= pi.et - interval '30' minute " +
                        "and od.et <= pi.et + interval '5' second " +
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type = dic.dic_code"
        );
        // 6 save data
        tEnv.executeSql(
                "create table dwd_trade_pay_detail_suc(" +
                        "order_detail_id string, " +
                        "order_id string, " +
                        "user_id string, " +
                        "sku_id string, " +
                        "sku_name string, " +
                        "province_id string, " +
                        "activity_id string, " +
                        "activity_rule_id string, " +
                        "coupon_id string, " +
                        "payment_type_code string, " +
                        "payment_type_name string, " +
                        "callback_time string, " +
                        "sku_num string, " +
                        "split_original_amount string, " +
                        "split_activity_amount string, " +
                        "split_coupon_amount string, " +
                        "split_payment_amount string, " +
                        "ts bigint" +
                        ")" +SQlUtil.getKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC)

        );
        result.executeInsert("dwd_trade_pay_detail_suc");



    }
}
