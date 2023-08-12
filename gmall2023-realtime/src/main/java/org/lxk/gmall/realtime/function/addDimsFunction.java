package org.lxk.gmall.realtime.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.util.BeanUtil;
import org.lxk.gmall.realtime.bean.TradeSkuOrderBean;
import org.lxk.gmall.realtime.util.HbaseUtil;
import org.lxk.gmall.realtime.util.RedisUtil;
import redis.clients.jedis.Jedis;

import java.util.List;

public abstract class addDimsFunction<T> extends RichMapFunction<T,T> implements DimFunction<T> {


    private Connection connection;
    private Jedis jedisclient;
    private JSONObject dimRow;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HbaseUtil.getConnection();
        jedisclient = RedisUtil.getJedisclient(RedisUtil.jedisPool);

    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeConnection(connection);
        RedisUtil.closeJedisClint(jedisclient);
    }


    @Override
    public T map(T bean) throws Exception {

        // get dimRow in redis first
         dimRow = RedisUtil.getDimRow(jedisclient, getTableStr(), getRowKey(bean), JSONObject.class);
        if (dimRow == null) {
        dimRow = HbaseUtil.<JSONObject>getRow(connection,
                "gmall",
                getRowKey(bean),
                getTableStr(),
                JSONObject.class);

        RedisUtil.WriteRow(jedisclient,getTableStr(),getRowKey(bean),dimRow);
        }
        addDim(bean, dimRow);
        return bean;
    }




}
