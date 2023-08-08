package org.lxk.gmall.realtime.util;

import org.lxk.gmall.realtime.common.GmallConstant;

public class SQlUtil {
    public static String getKafkaSourceSql(String groupId, String topic,String ... format) {

        String defaultFormat = "json"      ;
        if (format.length> 0) {
            defaultFormat=format[0];
        }
        return"WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = '"+topic+"'," +
                " 'properties.bootstrap.servers' = '"+ GmallConstant.KAFAK_BROCKERS +"'," +
                " 'properties.group.id' = '"+groupId+"'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                ("json".equals(defaultFormat)? " 'json.ignore-parse-errors' = 'true',":"")+
                "  'format' = '"+defaultFormat+"'" +
                ")" ;
    }
    public static String getKafkaSinkSql( String topic,String ... format) {

        String defaultFormat = "json"      ;
        if (format.length> 0) {
            defaultFormat=format[0];
        }
        return"WITH (" +
                " 'connector' = 'kafka'," +
                " 'topic' = '"+topic+"'," +
                " 'properties.bootstrap.servers' = '"+ GmallConstant.KAFAK_BROCKERS +"'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                ("json".equals(defaultFormat)? " 'json.ignore-parse-errors' = 'true',":"")+
                "  'format' = '"+defaultFormat+"'" +
                ")" ;
    }

    public static String getUpsertKafka( String topic, String ... format) {
        String defaultFormat = "json"      ;
        if (format.length> 0) {
            defaultFormat=format[0];
        }
        return"WITH (" +
                " 'connector' = 'upsert-kafka'," +
                " 'topic' = '"+topic+"'," +
                " 'properties.bootstrap.servers' = '"+ GmallConstant.KAFAK_BROCKERS +"'," +
                ("json".equals(defaultFormat)? " 'key.json.ignore-parse-errors' = 'true',":"")+
                ("json".equals(defaultFormat)? " 'value.json.ignore-parse-errors' = 'true',":"")+
                "  'key.format' = '"+defaultFormat+"'," +
                "  'value.format' = '"+defaultFormat+"'" +
                ")" ;
    }

    public static String getDorisSinkSql(String Table) {
        return "with(" +
                " 'connector' = 'doris'," +
                "  'fenodes' = 'hadoop162:7030'," +
                "  'table.identifier' = '"+Table+"'," +
                "  'username' = 'root'," +
                "  'password' = 'aaaaaa'," +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " +
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")";
    }
}
