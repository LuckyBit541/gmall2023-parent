package org.lxk.gmall.realtime.util;

//import redis.clients.jedis.JedisPool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.resource.ClientResources;
import org.lxk.gmall.realtime.common.GmallConstant;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.ExecutionException;

public class RedisUtil {

    public static JedisPool jedisPool;

    static {
        JedisPoolConfig poolconfig = new JedisPoolConfig();
        poolconfig.setMaxIdle(20);
        poolconfig.setMinIdle(10);
        poolconfig.setMaxWaitMillis(10000);
        poolconfig.setMaxTotal(200);
        jedisPool = new JedisPool(poolconfig, "hadoop162", 6379);
    }

    public static <T> T getDimRow(Jedis jedisclient, String table, String rowKey, Class<T> tClass) {
        String row = jedisclient.get(getKey(table, rowKey));
        if (row != null) {
            return JSON.parseObject(row, tClass);
        }


        return null;
    }

    private static String getKey(String table, String rowKey) {
        return table + ":" + rowKey;
    }

    public static void closeJedisClint(Jedis jedisclient) {
        if (jedisclient != null) {
            jedisclient.close();
        }
    }

    public static Jedis getJedisclient(JedisPool jedisPool) {
        Jedis client = jedisPool.getResource();
        client.select(4);
        return client;
    }

    public static void WriteRow(Jedis jedisclient, String tableStr, String rowKey, JSONObject dimRow) {
        jedisclient.setex(getKey(tableStr, rowKey), GmallConstant.TOW_DAYS, JSON.toJSONString(dimRow));
    }

    public static RedisClient getAsyncClient() {
        return RedisClient.create("redis://hadoop162:6379/4");
    }

    public static void getAsyncConnection(RedisClient redisClient) {

    }

    public static JSONObject getAsyncDimRow(StatefulRedisConnection<String, String> redisConnect, String tableStr, String rowKey, Class<JSONObject> tclas) {
        RedisAsyncCommands<String, String> commands = redisConnect.async();
        RedisFuture<String> redisFuture = commands.get(getKey(tableStr, rowKey));
        String rowDim;
        try {
            rowDim= redisFuture.get();
            if (rowDim != null) {
                return JSON.parseObject(rowDim,tclas);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


        return null;

    }
}
