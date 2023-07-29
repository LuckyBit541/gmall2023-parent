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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TableProcess;
import org.lxk.gmall.realtime.common.GmallConstant;

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
        SingleOutputStreamOperator<TableProcess> tp = readTableProcess(executionEnvironment);
        creatHbashTable(tp);
    }

    private void creatHbashTable(SingleOutputStreamOperator<TableProcess> tp) {

        return tp.map(new MapFunction<TableProcess, TableProcess>() {

            @Override
            public TableProcess map(TableProcess tableProcess) throws Exception {
                return null;
            }
        })
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
                        if ("d".equals(op)){
                             tp= jsonObject.getObject("before", TableProcess.class);

                        }else {
                            tp=jsonObject.getObject("after", TableProcess.class);

                        }
                        tp.setOp(op);

                        return null;
                    }
                })
                .filter(tp->"dim".equals(tp.getSinkType()));

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
