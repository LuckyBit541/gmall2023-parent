package org.lxk.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.util.KafkaUtil;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DwdTrafficLog extends BaseApp {
    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String PAGE = "page";
    private final String ACTION = "action";

    public static void main(String[] args) {
        new DwdTrafficLog().start(30001, 2, "DwdTrafficLog", GmallConstant.ODS_LOG);
    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        // 1 过滤数据
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        // 2 修正日志数据中 is_new 标记统计误差

        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(etledStream);
        // 3 分流，一个流按日志类型分为5个流
        System.out.println("DwdTrafficLog.handle");

        Map<String, DataStream<JSONObject>> streamMap = splitStream(validatedStream);
        // 4 把流写入到 kafka 中
        writeToKafka(streamMap);

    }

    private void writeToKafka(Map<String, DataStream<JSONObject>> streamMap) {

        streamMap.get(START).map(JSONAware::toJSONString).sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_START));
        streamMap.get(DISPLAY).map(JSONAware::toJSONString).sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap.get(ACTION).map(JSONAware::toJSONString).sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_ACTION));
        streamMap.get(ERR).map(JSONAware::toJSONString).sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap.get(PAGE)
                .map(JSONAware::toJSONString)
                .sinkTo(KafkaUtil.getKafkaSink(GmallConstant.TOPIC_DWD_TRAFFIC_PAGE));
    }

    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> stream) {
        OutputTag<JSONObject> dispalyTag = new OutputTag<JSONObject>("dispalys", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> actiontag = new OutputTag<JSONObject>("dispalys") {
        };
        OutputTag<JSONObject> errorTag = new OutputTag<JSONObject>("dispalys") {
        };
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("dispalys") {
        };

        SingleOutputStreamOperator<JSONObject> splitSteam = stream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {


                JSONObject common = value.getJSONObject("common");
                JSONObject start = value.getJSONObject("start");
                Long ts = value.getLong("ts");
                // 1 start
                if (start != null) {
                    collector.collect(start);
                }
                // 2 dispalys
                JSONArray displays = value.getJSONArray("displays");
                if (displays != null) {
                    for (int i = 0; i < displays.size(); i++) {

                        JSONObject jsonObj = displays.getJSONObject(i);
                        jsonObj.putAll(common);
                        jsonObj.put("ts", ts);
                        context.output(dispalyTag, jsonObj);
                    }
                    value.remove("displays");

                }
                // 3 actions
                JSONArray actions = value.getJSONArray("actions");
                if (actions != null) {

                    for (int i = 0; i < actions.size(); i++) {
                        JSONObject jsonObject = actions.getJSONObject(i);
                        jsonObject.putAll(common);
                        jsonObject.put("ts", ts);
                        context.output(actiontag, jsonObject);
                    }
                    value.remove("actions");
                }
                // 4 error
                JSONObject err = value.getJSONObject("err");
                if (err != null) {

                    context.output(errorTag, err);
                    value.remove("err");
                }

                // 5 page
                JSONObject page = value.getJSONObject("page");
                if (page != null) {
                    context.output(pageTag, page);
                }


            }
        });
        Map<String, DataStream<JSONObject>> map = new HashMap<>();
        map.put(START, splitSteam);
        map.put(DISPLAY, splitSteam.getSideOutput(dispalyTag));
        map.put(ACTION, splitSteam.getSideOutput(actiontag));
        map.put(ERR, splitSteam.getSideOutput(errorTag));
        map.put(PAGE, splitSteam.getSideOutput(pageTag));
        return map;

    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> etledStream) {
        // 0 create state
        return etledStream
                .keyBy(v -> v.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> isTruelyNew;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isTruelyNew = getRuntimeContext().getState(new ValueStateDescriptor<String>("isTruelyNew", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        // 1 is_new = 1
                        JSONObject common = jsonObject.getJSONObject("common");
                        String isNew = common.getString("is_new");
                        Long ts = jsonObject.getLong("ts");
                        String date = DateFormatUtils.format(new Date(ts), "yyyy-MM-dd");
                        if ("1".equals(isNew)) {
                            if (isTruelyNew.value() == null) {
                                isTruelyNew.update(date);
                            } else if (!isTruelyNew.value().equals(date)) {
                                common.put("is_new", "0");
                            }
                        }// 2 is_new = 0
                        else if ("0".equals(isNew)) {
                            common.put("ts", ts - 24 * 60 * 60 * 1000);
                        }
                        collector.collect(jsonObject);
                    }
                });


    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                try {
                    return jsonObject.containsKey("actions")
                            || jsonObject.containsKey("start")
                            || jsonObject.containsKey("err")
                            || jsonObject.containsKey("displays")
                            || jsonObject.containsKey("page");
                } catch (Exception e) {
                    log.warn("错误日志数据:" + s);
                    return false;
                }

            }
        }).map(JSON::parseObject);
    }
}
