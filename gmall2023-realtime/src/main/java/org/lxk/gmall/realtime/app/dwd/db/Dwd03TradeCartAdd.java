package org.lxk.gmall.realtime.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.SQlUtil;

public class Dwd03TradeCartAdd extends BaseSqlApp {
    public static void main(String[] args) {
        new Dwd03TradeCartAdd().start(30002, 2, "Dwd02InteractionCommentInfo");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        // 1 read data from ods_db topic of Kafka
        readOdsDb(tEnv, "Dwd02InteractionCommentInfo");
        // 2 filter comment data out of raw data

        Table cart_info = tEnv.sqlQuery(
                "select " +
                        "data['id'] id, " +
                        "data['user_id'] user_id, " +
                        "data['sku_id'] sku_id, " +
                        "if(`type`='insert',data['sku_num'] ," +
                        "cast(cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int) as string) ) sku_num, " +
                        "ts "+
                        "from ods_db " +
                        "where `database`= 'gmall2023' " +
                        "and `table` = 'cart_info' " +
                        "and `type`='insert' " +
                        "or( `type`='update' " +
                        "and `old`['sku_num'] is not null " +
                        "and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)" +
                        ") "
        );
        tEnv.createTemporaryView("cart_info", cart_info);
        tEnv.executeSql("create table dwd_trade_cart_add (" +
                "    id string," +
                "    user_id string," +
                "    sku_id string," +
                "    sku_num string," +
                "    ts bigint) " + SQlUtil.getKafkaSinkSql(GmallConstant.TOPIC_DWD_TRADE_CART_ADD));


        cart_info.executeInsert("dwd_trade_cart_add");

    }
}
