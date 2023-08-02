package org.lxk.gmall.realtime.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.util.SQlUtil;

import java.time.Duration;

public class ConsumeFromKafka extends BaseSqlApp {

    public static void main(String[] args) {
        new ConsumeFromKafka().start(40001, 2,"InnerJoin" );
    }
    @Override


    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(4));
        TableResult t1 = tEnv.executeSql("create table t1( " +
                "id int," +
                "name string " +
                ")" + SQlUtil.getKafkaSourceSql("InnerJoin1", "t1", "csv"));
        tEnv.sqlQuery("select * from t1").execute().print();


    }
}
