package org.lxk.gmall.realtime.util;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.connector.sink2.Sink;

import java.util.Properties;

/**
 * @Author:LB
 * @Version:1.0
 */
public class DorisUtil {
    public static DorisSink<String> getDorisSink(String table) {

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        return DorisSink
                .<String>builder()
                .setDorisReadOptions(
                        DorisReadOptions
                                .builder()
                                .build()
                )
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes("hadoop162:7030")
                                .setTableIdentifier(table)
                                .setUsername("root")
                                .setPassword("aaaaaa")
                                .build()
                )
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder()
                                .setBufferSize(100)
                                .setCheckInterval(1000)
                                .setBufferCount(10)
                                .setMaxRetries(3)
                                .setStreamLoadProp(props)
                                .disable2PC()
                                .build())
                .setSerializer(new SimpleStringSerializer()
                )
                .build();

    }
}
