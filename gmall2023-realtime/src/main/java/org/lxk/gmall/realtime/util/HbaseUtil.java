package org.lxk.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.lxk.gmall.realtime.bean.TableProcess;
import org.lxk.gmall.realtime.function.HbaseSink;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @Author:LB
 * @Version:1.0
 */
@Slf4j
public class HbaseUtil {

    public static Connection getConnection() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop162");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }

    public static void closeConnection(Connection connection) {
        try {
            if (connection != null) {

                connection.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static void createTabel(Connection connection, String namespace, String tablename, String columnFamily) {
        TableName tableName = TableName.valueOf(namespace, tablename);
        try (Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(tableName)) {
                ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build();
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(familyDescriptor).build();
                admin.createTable(tableDescriptor);
                log.info("=====create table " + tablename + " in Hbase=====");

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteTable(Connection connection, String nameSpace, String table) {
        TableName tableName = TableName.valueOf(nameSpace, table);
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                log.info("=====delete table " + table + " in Hbase=====");

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getHbaseSink() {
        return new HbaseSink();
    }


    public static void putOneRow(Connection connection, String nameSpace, String sinkTable, String sinkFamily, String[] sinkColumns, String rowKey, JSONObject data) {
        TableName tableName = TableName.valueOf(nameSpace, sinkTable);

        try (Table table = connection.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            for (String sinkColumn : sinkColumns) {
                String cell = data.getString(sinkColumn);
                if (cell != null) {
                    put.addColumn(Bytes.toBytes(sinkFamily),
                            Bytes.toBytes(sinkColumn),
                            Bytes.toBytes(cell));
                }
            }
            table.put(put);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteOneRow(Connection connection, String nameSpace, String sinkTable, String rowKey) {
        TableName tableName = TableName.valueOf(nameSpace, sinkTable);
        try (Table table = connection.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T getRow(Connection connection, String nameSpace, String rowKey, String hbaseTableName, Class<T> tClass) {
        TableName tableName = TableName.valueOf(nameSpace, hbaseTableName);
        // System.out.println(tableName);
        try (Table table = connection.getTable(tableName)) {
            //System.out.printf("====================================="+rowKey);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            T t = tClass.newInstance();

            for (Cell cell : cells) {
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                BeanUtils.setProperty(t, columnName, value);

            }
            return t;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    public static AsyncConnection getAsyncHbaseConnection() {
        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "hadoop162");
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");

        AsyncConnection asyncConnection;
        try {
            asyncConnection = ConnectionFactory.createAsyncConnection(hbaseConfig).get();
            //System.out.println("conn:"+asyncConnection);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        //System.out.println("conn:"+asyncConnection);
        return asyncConnection;
    }

    public static void closeAsyncConnection(AsyncConnection hbaseAsyncConnection) {
        try {
            hbaseAsyncConnection.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    public static <T> T getAsyncDimRow(AsyncConnection hbaseAsyncConnection,String nameSpace,String tableStr, String rowKey, Class<T> tClass) {

        TableName tableName = TableName.valueOf(nameSpace, tableStr);
        AsyncTable<AdvancedScanResultConsumer> table = hbaseAsyncConnection.getTable(tableName);

        Get get = new Get(Bytes.toBytes(rowKey));
        CompletableFuture<Result> resultCompletableFuture = table.get(get);

        try {
            Result result = resultCompletableFuture.get();
            List<Cell> cells = result.listCells();
            T tBean = tClass.newInstance();
            for (Cell cell : cells) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                BeanUtils.setProperty(tBean,key,value);
            }
            return tBean;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


    }
}
