package org.lxk.gmall.realtime.join;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lxk.gmall.realtime.app.BaseApp;

public class ConsumeKafakLeftJoin extends BaseApp {

    public static void main(String[] args) {
        new ConsumeKafakLeftJoin().start(40089, 2, "ConsumeKafakLeftJoin", "t4");
    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        stream.print();
    }
}
