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
                " 'format' = '"+defaultFormat+"'," +
                ("json".equals(defaultFormat)? " 'json.ignore-parse-errors' = 'true'":"")+
                ")" ;
    }
}
