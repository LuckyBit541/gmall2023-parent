package org.lxk.gmall.realtime.function;

import com.sun.xml.internal.ws.util.CompletedFuture;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.lxk.gmall.realtime.util.HbaseUtil;
import org.lxk.gmall.realtime.util.RedisUtil;

import java.util.concurrent.CompletableFuture;

public class AsyncRichDimFunction<T> extends RichAsyncFunction<T,T> {

    private StatefulRedisConnection<String, String> connect;
    private AsyncConnection asyncConnection;
    private RedisClient redisClient;

    //https://lettuce.io/docs/getting-started.html
    @Override
    public void open(Configuration parameters) throws Exception {
        // get redis async connection
        redisClient = RedisUtil.getAsyncClient();
        connect = redisClient.connect();
        // get hbase connection
        asyncConnection = HbaseUtil.getAsyncConnection();

    }
    @Override
    public void close() throws Exception {
        if (connect != null) {
            connect.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }

        if (asyncConnection != null) {
            asyncConnection.close();
        }
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        CompletableFuture.supplyAsync();

    }
}
