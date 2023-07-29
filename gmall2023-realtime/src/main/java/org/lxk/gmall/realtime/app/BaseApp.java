package org.lxk.gmall.realtime.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lxk.gmall.realtime.util.KafkaUtil;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {
public void start(int port, int p, String ckAndGroupIdAndJobName, String topic){
    System.setProperty("HADOOP_USER_NAME", "atguigu");
    Configuration conf = new Configuration();
    conf.setInteger("rest.port", port);
    conf.setString("pipeline.name","DimApp" );

    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    executionEnvironment.setParallelism(p)
            .enableCheckpointing(3000)
            .setStateBackend(new HashMapStateBackend());
    executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    executionEnvironment.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/ck/"+ckAndGroupIdAndJobName);
    executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    executionEnvironment.getCheckpointConfig().setCheckpointTimeout(1000);
    executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

    KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(ckAndGroupIdAndJobName,topic);
    DataStreamSource<String> stream = executionEnvironment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
    handle(executionEnvironment,stream);
    try {
        executionEnvironment.execute();
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
}

    protected abstract void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream );

}
