package org.lxk.gmall.realtime.function;

import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.ClientResources;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.lxk.gmall.realtime.util.HbaseUtil;
import org.lxk.gmall.realtime.util.RedisUtil;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AsyncRichDimFunction<T> extends RichAsyncFunction<T,T> implements DimFunction<T> {

    private StatefulRedisConnection<String, String> redisConnect;
    private AsyncConnection hbaseAsyncConnection;
    private RedisClient redisClient;

    //https://lettuce.io/docs/getting-started.html
    @Override
    public void open(Configuration parameters) throws Exception {
        // get redis async connection
        redisClient = RedisUtil.getAsyncClient();
        // redisConnect = RedisUtil.getRedisAsyncClient(redisClient);
      redisConnect = redisClient.connect();
        // get hbase connection
        hbaseAsyncConnection = HbaseUtil.getAsyncHbaseConnection();

    }
    @Override
    public void close() throws Exception {
        if (redisConnect != null) {
            redisConnect.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }

        HbaseUtil.closeAsyncConnection(hbaseAsyncConnection);
    }

    @Override
    public void asyncInvoke(T bean, ResultFuture<T> resultFuture) throws Exception {
        CompletableFuture
                .supplyAsync(new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                       JSONObject dimRow= RedisUtil.getAsyncDimRow(redisConnect,getTableStr(),getRowKey(bean),JSONObject.class);
                        return dimRow;
                    }
                })
                .thenApplyAsync(new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimRow) {
                        if (null==dimRow) {
                            dimRow=HbaseUtil.<JSONObject>getAsyncDimRow(hbaseAsyncConnection,"gmall",getTableStr(),getRowKey(bean),JSONObject.class);
                            RedisUtil.asyncWriteRow(redisConnect,getTableStr(),getRowKey(bean),dimRow);
                        }
                        return dimRow;
                    }
                })
                .thenAccept(new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimRow) {
                       addDim(bean,dimRow);
                       resultFuture.complete(Collections.singleton(bean));
                    }
                });



    }


}
