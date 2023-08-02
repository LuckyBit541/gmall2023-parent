package org.lxk.gmall.realtime.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.lxk.gmall.realtime.common.GmallConstant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class KafkaUtil {
    public static KafkaSource<String> getKafkaSource(String groupid, String topic) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(GmallConstant.KAFAK_BROCKERS)
                .setTopics(topic)
                .setGroupId(groupid)
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] massage) throws IOException {
                        if (massage != null) {
                            return new String(massage, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(new TypeHint<String>(){});
                    }
                })
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        return source;
    }

    public static Sink<String> getKafkaSink(String sinkTopic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(GmallConstant.KAFAK_BROCKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .<String>builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(sinkTopic)
                .setProperty("transaction.timeout.ms", 10  * 60 * 1000 + "")
                .build();

    }
}
