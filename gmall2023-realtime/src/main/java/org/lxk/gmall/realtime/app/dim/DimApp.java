package org.lxk.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.common.GmallConstant;

@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(20001, 2, "DimApp", GmallConstant.TOP_ODS_DB);

    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        etl(stream);
    }

    private void etl(DataStreamSource<String> stream) {
        stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {

                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            Object type = jsonObject.getString("type");
                            String data = jsonObject.getString("data");
                            return "gmall2023".equals(jsonObject.getString("database"))
                                    && null != jsonObject.getString("table")
                                    && null != jsonObject.getString("ts")
                                    && ("insert".equals(type) || "update".equals(type) || "delete".equals(type))
                                    && null != data
                                    && data.length() > 2;


                        } catch (Exception e) {
                            log.warn("数据格式不是json:" + value);
                            return false;
                        }

                    }
                })
                .print();
    }
}
