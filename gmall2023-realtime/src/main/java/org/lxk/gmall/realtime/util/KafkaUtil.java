package org.lxk.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.common.utils.Bytes;
import org.lxk.gmall.realtime.bean.TableProcess;
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
                .setStartingOffsets(OffsetsInitializer.earliest())
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
                .setTransactionalIdPrefix(sinkTopic+new Random().nextInt())
                .setProperty("transaction.timeout.ms", 10  * 60 * 1000 + "")
                .build();

    }
    public static Sink<Tuple2<JSONObject, TableProcess>> getKafkaSink() {
        return KafkaSink.<Tuple2<JSONObject, TableProcess>>builder()
                .setBootstrapServers(GmallConstant.KAFAK_BROCKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .<Tuple2<JSONObject, TableProcess>>builder()
                                .setTopicSelector(t -> t.f1.getSinkTable())
                                .setValueSerializationSchema(new SerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                                    @Override
                                    public byte[] serialize(Tuple2<JSONObject, TableProcess> data) {
                                        return data.f0.toJSONString().getBytes(StandardCharsets.UTF_8);
                                    }
                                })
                                //.setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("lxk"+new Random().nextInt())
                .setProperty("transaction.timeout.ms", 10  * 60 * 1000 + "")
                .build();

    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSinkBySchema(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream) {
        return null;
    }
}
