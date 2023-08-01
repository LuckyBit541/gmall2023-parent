package org.lxk.gmall.realtime.app.dwd.db;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.app.BaseSqlApp;

public class DwdInteractionCommentInfo extends BaseSqlApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(30002, 2, "DwdInteractionCommentInfo" );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        readOdsDb(tEnv, "DwdInteractionCommentInfo");
        tEnv.sqlQuery("select * from ods_db").execute().print();

    }
}
