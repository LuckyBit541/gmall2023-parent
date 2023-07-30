package org.lxk.gmall.realtime.app.dim;

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

@Slf4j
public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(20001, 2, "DimApp", GmallConstant.TOP_ODS_DB);

    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        //todo 1 对数据进行清洗
        SingleOutputStreamOperator<String> etledStream = etl(stream);
        //todo 2 cdc 读取配置文件
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(executionEnvironment);
        //todo 3 在 Hbase 中建表
        tpStream = creatHbashTable(tpStream);
        //todo 4 tpstream and dataStream connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimDataAndTpStream = connect(etledStream, tpStream);

        dimDataAndTpStream.print();


    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<String> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        // 1 创建广播状态
        MapStateDescriptor<String, TableProcess> descriptor = new MapStateDescriptor<>("tp", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBroadcast = tpStream.broadcast(descriptor);
        // 2 dataStream connect with broadStream
       return dataStream
                .connect(tpBroadcast)
                .process(new BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    //处理数据流
                    @Override
                    public void processElement(String jsonStr, BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(descriptor);
                        TableProcess tp = broadcastState.get(jsonObject.getString("table") + "ALL");
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
                        }else {
                            broadcastState.put(key,tp);
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
                .databaseList(GmallConstant.CONFIG_DATABASE + ".table_process")
                .username(GmallConstant.MYSQL_USERNAME)
                .password(GmallConstant.MYSQ_PASSWD)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        return executionEnvironment.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "configSource")
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

                        return null;
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
                                    && ("insert".equals(type) || "update".equals(type) || "delete".equals(type))
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
