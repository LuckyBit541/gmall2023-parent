package org.lxk.gmall.realtime.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.util.SQlUtil;

import java.time.Duration;

public class LookUpJoin extends BaseSqlApp {

    public static void main(String[] args) {
        new LookUpJoin().start(40001, 2, "InnerJoin");
    }

    @Override


    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(4));
        TableResult t1 = tEnv.executeSql("create table t1( " +
                "id string," +
                "pt as proctime() " +
                ")" + SQlUtil.getKafkaSourceSql("InnerJoin1", "t1", "csv"));
        tEnv.executeSql("create table base_dic(" +
                "id string ," +
                "info row<dic_name string> )with" +
                "(" +
                " 'connector' = 'hbase-2.2'," +
                " 'table-name' = 'gmall:dim_base_dic'," +
                "'lookup.partial-cache.expire-after-write' = '20 second'," +
                " 'lookup.partial-cache.expire-after-access' = '20 second'," +
                " 'lookup.partial-cache.max-rows' = '20', " +
                " 'zookeeper.quorum' = 'hadoop162,hadoop163,hadoop164:2181' " +
                ")"
        );
        tEnv.sqlQuery(
                "select t1.id , dic.dic_name from " +
                        "t1 left join base_dic for system_time as of t1.pt as dic " +
                        "on t1.id=dic.id").execute().print();


    }
}
