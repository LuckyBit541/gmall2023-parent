package org.lxk.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.lxk.gmall.realtime.bean.TableProcess;
import org.lxk.gmall.realtime.function.HbaseSink;

import java.io.IOException;

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
            connection.close();
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
                log.info("=====create table "+tablename+" in Hbase=====");

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteTable(Connection connection, String nameSpace, String table) {
        TableName tableName = TableName.valueOf(nameSpace,table);
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                log.info("=====delete table "+table+" in Hbase=====");

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getHbaseSink() {
        return new HbaseSink();
    }


    public static void putOneRow(Connection connection, String nameSpace, String sinkTable, String sinkFamily, String[] sinkColumns, String rowKey, JSONObject data) {
        TableName tableName = TableName.valueOf(nameSpace,sinkTable);

        try (Table table = connection.getTable(tableName)){
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
        try (Table table=connection.getTable(tableName)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
