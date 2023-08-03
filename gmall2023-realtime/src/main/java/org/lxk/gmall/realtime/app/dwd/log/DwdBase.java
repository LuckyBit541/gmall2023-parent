package org.lxk.gmall.realtime.app.dwd.log;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.common.GmallConstant;

import javax.swing.text.GapContent;

/**
 * @Author:LB
 * @Version:1.0
 */
public class DwdBase extends BaseApp {
    public static void main(String[] args) {
        new DwdBase().start(30001,2,"DwdBase", GmallConstant.ODS_LOG);
    }
    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        

    }
}
