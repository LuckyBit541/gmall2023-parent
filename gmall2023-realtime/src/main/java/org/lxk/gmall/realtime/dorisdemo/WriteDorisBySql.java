package org.lxk.gmall.realtime.dorisdemo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;

public class WriteDorisBySql extends BaseSqlApp {
    public static void main(String[] args) {
        new WriteDorisBySql().start(30011,2 ,"ReadDorisBySql" );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "create table student(" +
                        "name string," +
                        "age int" +
                        ")with(" +
                        " 'connector' = 'doris',  " +
                        " 'fenodes' = 'hadoop162:7030',  " +
                        " 'table.identifier' = 'test1.student',  " +
                        " 'username' = 'root',  " +
                        " 'password' = 'aaaaaa'  ," +
                        "  'sink.properties.format' = 'json', " +
                        "  'sink.buffer-count' = '4', " +
                        "  'sink.buffer-size' = '4086'," +
                        "  'sink.enable-2pc' = 'false', " +
                        "  'sink.properties.read_json_by_line' = 'true' " +
                        ")"
        );
        tEnv.executeSql("insert into student values('Tim',18)");

    }
}
