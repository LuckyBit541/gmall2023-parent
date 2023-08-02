package org.lxk.gmall.realtime.join;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.util.KafkaUtil;
import org.lxk.gmall.realtime.util.SQlUtil;

public class ConsumeUpsertKafakLeftJoin extends BaseSqlApp {

    public static void main(String[] args) {
        new ConsumeUpsertKafakLeftJoin().start(40086, 2, "ConsumeUpsertKafakLeftJoin");
    }

        @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
            tEnv.executeSql("create table t12(" +
                    " id string , " +
                    " name string, " +
                    " age int, " +
                    " primary key(id) NOT ENFORCED" +
                    ")" + SQlUtil.getUpsertKafka("t4", "json"));
            tEnv.sqlQuery("select * from t12").execute().print();
    }
}
