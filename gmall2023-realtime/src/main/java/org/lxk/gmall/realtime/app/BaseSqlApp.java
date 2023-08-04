package org.lxk.gmall.realtime.app;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.SQlUtil;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseSqlApp {
public void start(int port, int p, String ckAndJobName){
    // *** 设置环境变量
    System.setProperty("HADOOP_USER_NAME", "atguigu");

    Configuration conf = new Configuration();
    conf.setInteger("rest.port", port);
    conf.setString("pipeline.name","DimApp" );

    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    executionEnvironment.setParallelism(p)
            .enableCheckpointing(3000)
            .setStateBackend(new HashMapStateBackend());
    executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    executionEnvironment.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/ck/"+ckAndJobName);
    executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
    executionEnvironment.getCheckpointConfig().setCheckpointTimeout(10000);
    executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

    StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

    handle(executionEnvironment,streamTableEnvironment);
  /*  try {
        executionEnvironment.execute();
    } catch (Exception e) {
        throw new RuntimeException(e);
    }*/
}

    protected abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv );
public void readOdsDb(StreamTableEnvironment tEnv , String groupId){
    tEnv.executeSql(
            "create table ods_db(" +
                    "`database` string, " +
                    "`table` string, " +
                    "`type` string, " +
                    "`data` map<string, string>, " +
                    "`old` map<string, string>, " +
                    "`ts` bigint ," +
                    "pt as proctime() ," +
                    "et as to_timestamp_ltz(ts,0) ," +
                    "watermark for et as et - interval '3' second" +
                    ")"+ SQlUtil.getKafkaSourceSql(groupId, GmallConstant.ODS_DB)

    );


}
public void readBaseDic(StreamTableEnvironment tEnv , String groupId){
    tEnv.executeSql("create table base_dic(" +
            "dic_code string ," +
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
}

}
