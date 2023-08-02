package org.lxk.gmall.realtime.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;
import org.lxk.gmall.realtime.util.SQlUtil;

import java.time.Duration;

public class InnerJoinToKafka extends BaseSqlApp {

    public static void main(String[] args) {
        new InnerJoinToKafka().start(40001, 2,"InnerJoin" );
    }
    @Override


    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(4));
        TableResult t1 = tEnv.executeSql("create table t1( " +
                "id int," +
                "name string " +
                ")" + SQlUtil.getKafkaSourceSql("InnerJoin1", "t1", "csv"));
        //tEnv.sqlQuery("select * from t1").execute().print();
        tEnv.executeSql("create table t2( " +
                "id int," +
                "age int " +
                ")"+ SQlUtil.getKafkaSourceSql("InnerJoin","t2", "csv"));

        //tEnv.sqlQuery("select * from t2").execute().print();
        Table t3 = tEnv.sqlQuery("select t1.id , t1.name , t2.age from " +
                "t1 join t2 " +
                "on t1.id= t2.id");
        tEnv.createTemporaryView("t3",t3);
        tEnv.executeSql("create table t4( " +
                "id int," +
                "name string," +
                "age int " +
                ")"+ SQlUtil.getKafkaSourceSql("InnerJoin4","t4", "json"));
        t3.executeInsert("t4");

    }
}
