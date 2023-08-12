package org.lxk.gmall.realtime.util;

//import redis.clients.jedis.JedisPool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import org.lxk.gmall.realtime.common.GmallConstant;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    public static JedisPool jedisPool;

    static {
        JedisPoolConfig poolconfig = new JedisPoolConfig();
        poolconfig.setMaxIdle(20);
        poolconfig.setMinIdle(10);
        poolconfig.setMaxWaitMillis(10000);
        poolconfig.setMaxTotal(200);
        jedisPool = new JedisPool(poolconfig,"hadoop162",6379);
    }

    public static <T> T getDimRow(Jedis jedisclient, String table, String rowKey, Class<T> tClass) {
        String row = jedisclient.get(getKey(table, rowKey));
        if (row != null) {
            return JSON.parseObject(row,tClass);
        }


        return null;
    }

    private static String getKey(String table, String rowKey) {
       return  table+":"+rowKey;
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
        jedisclient.setex(getKey(tableStr,rowKey), GmallConstant.TOW_DAYS,JSON.toJSONString(dimRow));
    }
//todo
    public static RedisClient getAsyncClient() {
        return null;
    }

    public static void getAsyncConnection(RedisClient redisClient) {

    }
}
