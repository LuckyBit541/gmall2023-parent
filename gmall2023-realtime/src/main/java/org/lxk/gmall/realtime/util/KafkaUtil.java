package org.lxk.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.lxk.gmall.realtime.common.GmallConstant;

public class KafkaUtil {
    public static KafkaSource<String> getKafkaSource(String groupid, String topic) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(GmallConstant.KAFAK_BROCKERS)
                .setTopics(topic)
                .setGroupId(groupid)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        return source;
    }
}
