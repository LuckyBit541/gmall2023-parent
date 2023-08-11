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

import java.util.List;

public abstract class addDimsFunction<T> extends RichMapFunction<T,T> implements DimFunction<T> {


    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HbaseUtil.getConnection();

    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeConnection(connection);
    }


    @Override
    public T map(T bean) throws Exception {
        JSONObject dimRow = HbaseUtil.<JSONObject>getRow(connection,
                "gmall",
                getRowKey(bean),
                getTableStr(),
                JSONObject.class);

        addDim(bean,dimRow);
        return bean;

    }



}
