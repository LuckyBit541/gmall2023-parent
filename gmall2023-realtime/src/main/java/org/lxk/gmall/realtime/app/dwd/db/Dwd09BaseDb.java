package org.lxk.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TableProcess;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.HbaseUtil;
import org.lxk.gmall.realtime.util.JDBCUtil;
import org.lxk.gmall.realtime.util.KafkaUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class Dwd09BaseDb extends BaseApp {
    public static void main(String[] args) {
        new Dwd09BaseDb().start(30009, 2, "Dwd09BaseDb", GmallConstant.TOP_ODS_DB);

    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        // 1 对数据进行清洗
        SingleOutputStreamOperator<String> etledStream = etl(stream);
        // 2 cdc 读取配置文件
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(executionEnvironment);
        // 3 在 Hbase 中建表
       // tpStream = creatHbashTable(tpStream);
        // 4 tpstream and dataStream connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimDataAndTpStream = connect(etledStream, tpStream);
        //dimDataAndTpStream.print();
        //dimDataAndTpStream.print();
        // 5 filter redundant columns of data
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = deleteNotNeededColumns(dimDataAndTpStream);
        //resultStream.print();
        // 6 dataStream write down into Hbase
        writeToKafka(resultStream);


    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream) {
        resultStream.sinkTo(KafkaUtil.getKafkaSink());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> deleteNotNeededColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataStream) {
        return dataStream.map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> tuple2) throws Exception {
                JSONObject data = tuple2.f0;
                List<String> sinkCol = Arrays.asList((tuple2.f1.getSinkColumns() + ",op_type").split(","));
                data.keySet().removeIf(key -> !sinkCol.contains(key));
                return tuple2;
            }
        });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<String> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        // 1 创建广播状态
        MapStateDescriptor<String, TableProcess> descriptor = new MapStateDescriptor<>("tp", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBroadcast = tpStream.broadcast(descriptor);
        // 2 dataStream connect with broadStream
        return dataStream
                .connect(tpBroadcast)
                .process(new BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    private HashMap<String, TableProcess> map;


                    // 3 预加载配置数据
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        map = new HashMap<>();
                        // 3.1 获取jdbc连接
                        java.sql.Connection jdbcConnection = JDBCUtil.getJDBCConnection();
                        // 3.2 获取查询结果
                        List<TableProcess> tpList = JDBCUtil.qeuryList(
                                jdbcConnection,
                                "select * from gmall2023_config.table_process",
                                new Object[]{},
                                TableProcess.class,
                                true
                        );
                        // 3.3 查询结果封装到map中

                        for (TableProcess tableProcess : tpList) {
                            map.put(getKey(tableProcess.getSourceTable(), tableProcess.getSourceType()), tableProcess);
                        }
                        // 3.4 关闭连接
                        JDBCUtil.closeConnection(jdbcConnection);

                        //System.out.println("map:::::"+map);

                    }


                    // 1 处理数据流
                    @Override
                    public void processElement(String jsonStr, BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(descriptor);
                        String key = getKey(jsonObject.getString("table"), jsonObject.getString("type"));
                        TableProcess tp = broadcastState.get(key);
                        // tp为空,继续在tplist 预加载配置数据中查询
                        if (tp == null) {
                            System.out.println("=========not in state");
                            tp = map.get(key);
                            if (tp != null) {

                                System.out.println("=====[map中的配置] ======");
                            }
                        }
                        if (null != tp) {
                            JSONObject data = jsonObject.getJSONObject("data");
                            data.put("op_type", jsonObject.getString("type").replace("bootstrap-", ""));
                            collector.collect(Tuple2.of(data, tp));

                        }
                    }
                    //处理广播配置流

                    @Override
                    public void processBroadcastElement(TableProcess tp, BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>.Context context, Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {

                        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(descriptor);
                        String key = getKey(tp.getSourceTable(), tp.getSourceType());
                        if ("d".equals(tp.getOp())) {
                            broadcastState.remove(key);
                            map.remove(key);
                        } else {
                            broadcastState.put(key, tp);
                        }

                    }

                    private String getKey(String table, String type) {
                        return table + ":" + type;
                    }
                });


    }

    private SingleOutputStreamOperator<TableProcess> creatHbashTable(SingleOutputStreamOperator<TableProcess> tp) {

        return tp.map(new RichMapFunction<TableProcess, TableProcess>() {
            private Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取连接
                connection = HbaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                //关闭链接
                HbaseUtil.closeConnection(connection);
            }

            @Override
            public TableProcess map(TableProcess tb) throws Exception {
                String op = tb.getOp();
                if ("d".equals(op)) {
                    deleteTable(tb);
                } else if ("c".equals(op) || "r".equals(op)) {
                    createTable(tb);
                } else {
                    deleteTable(tb);
                    createTable(tb);
                }
                return tb;
            }

            private void createTable(TableProcess tb) {
                HbaseUtil.createTabel(connection, "gmall", tb.getSinkTable(), tb.getSinkFamily());
            }

            private void deleteTable(TableProcess tb) {
                HbaseUtil.deleteTable(connection, "gamll", tb.getSinkTable());
            }
        });
    }

    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment executionEnvironment) {
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(GmallConstant.MYSQL_HOSTNAME)
                .port(GmallConstant.MYSQL_PORT)
                .databaseList(GmallConstant.CONFIG_DATABASE)
                .tableList(GmallConstant.CONFIG_DATABASE + ".table_process")
                .username(GmallConstant.MYSQL_USERNAME)
                .password(GmallConstant.MYSQ_PASSWD)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        return executionEnvironment.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "configSource")
                .setParallelism(1)
                .map(new MapFunction<String, TableProcess>() {
                    @Override
                    public TableProcess map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String op = jsonObject.getString("op");
                        TableProcess tp;
                        if ("d".equals(op)) {
                            tp = jsonObject.getObject("before", TableProcess.class);

                        } else {
                            tp = jsonObject.getObject("after", TableProcess.class);

                        }
                        tp.setOp(op);

                        return tp;
                    }
                })
                .filter(tp -> "dim".equals(tp.getSinkType()));

    }

    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> stream) {
        return stream
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
                                    && ("insert".equals(type) || "update".equals(type) || "delete".equals(type) || "bootstrap-insert".equals(type))
                                    && null != data
                                    && data.length() > 2;


                        } catch (Exception e) {
                            log.warn("数据格式不是json:" + value);
                            return false;
                        }

                    }
                });
    }
}
