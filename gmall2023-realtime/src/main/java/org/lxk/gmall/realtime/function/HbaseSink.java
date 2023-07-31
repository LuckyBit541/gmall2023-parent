package org.lxk.gmall.realtime.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.lxk.gmall.realtime.bean.TableProcess;
import org.lxk.gmall.realtime.util.HbaseUtil;

public class HbaseSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {

        connection = HbaseUtil.getConnection();
    }

    @Override
    public void close() throws Exception {
       HbaseUtil.closeConnection(connection);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        JSONObject data = value.f0;
        String opType = data.getString("op_type");
        if ("insert".equals(opType)||"update".equals(opType)){
            putOneRow(value);
        }else if ("delete".equals(opType)){
            deleteOneRow(value);
        }
    }

    private void deleteOneRow(Tuple2<JSONObject, TableProcess> value) {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
        HbaseUtil.deleteOneRow(
                connection,
                "gmall",
                tp.getSinkTable(),
                data.getString(tp.getSinkRowKey())
        );
    }

    private void putOneRow(Tuple2<JSONObject, TableProcess> value) {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;
        HbaseUtil.putOneRow(
                connection,
                "gmall",
                tp.getSinkTable(),
                tp.getSinkFamily(),
                tp.getSinkColumns().split(","),
                data.getString(tp.getSinkRowKey()),
                data
                );
    }


}
