package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TrafficPageViewBean;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.DorisMapFunction;
import org.lxk.gmall.realtime.util.DorisUtil;

import java.time.Duration;

public class Dws_02_DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_02_DwsTrafficVcChArIsNewPageViewWindow().start(40002, 2, "Dws_02_DwsTrafficVcChArIsNewPageViewWindow", GmallConstant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> dataStream) {
        // 1 parse json as  pojo
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageView = parseStringToPojo(dataStream);
        // 2 aggregate
        SingleOutputStreamOperator<TrafficPageViewBean> aggregatedData = aggUvSvPvDurTime(trafficPageView);
        // 3 save data to doris
        saveToDoris(aggregatedData);


    }

    private void saveToDoris(SingleOutputStreamOperator<TrafficPageViewBean> dataStream) {
        dataStream
                .map(new DorisMapFunction<TrafficPageViewBean>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_traffic_vc_ch_ar_is_new_page_view_window"));


    }

    private SingleOutputStreamOperator<TrafficPageViewBean> aggUvSvPvDurTime(SingleOutputStreamOperator<TrafficPageViewBean> trafficPageView) {
        return trafficPageView
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                                        return trafficPageViewBean.getTs();
                                    }
                                })
                                .withIdleness(Duration.ofMinutes(1))
                )
                .keyBy(v->v.getVc()+"_"+v.getCh()+"_"+v.getAr()+"_"+v.getIsNew())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean bean1, TrafficPageViewBean bean2) throws Exception {
                        bean1.setUvCt(bean1.getUvCt() + bean2.getUvCt());
                        bean1.setSvCt(bean1.getSvCt() + bean2.getSvCt());
                        bean1.setPvCt(bean1.getPvCt() + bean2.getPvCt());
                        bean1.setDurSum(bean1.getDurSum() + bean2.getDurSum());
                        return bean1;
                    }
                }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                        TrafficPageViewBean result = iterable.iterator().next();
                        TimeWindow window = context.window();
                        result.setStt(DateFormatUtils.format(window.getStart(),"yyyy-MM-dd HH:mm:ss"));
                        result.setCur_date(DateFormatUtils.format(window.getStart(),"yyyy-MM-dd"));
                        result.setEdt(DateFormatUtils.format(window.getEnd(),"yyyy-MM-dd HH:mm:ss"));
                        collector.collect(result);
                    }
                });

    }

    private SingleOutputStreamOperator<TrafficPageViewBean> parseStringToPojo(DataStreamSource<String> dataStream) {
        return dataStream
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        String mid = JSON.parseObject(value).getJSONObject("common").getString("mid");

                        return mid;
                    }
                })
                .map(new RichMapFunction<String, TrafficPageViewBean>() {

                    private ValueState<String> isToday;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        isToday = getRuntimeContext().getState(new ValueStateDescriptor<String>("isToday", String.class));
                    }

                    @Override
                    public TrafficPageViewBean map(String json) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(json);
                        JSONObject common = jsonObject.getJSONObject("common");
                        String ch = common.getString("ch");
                        String vc = common.getString("vc");
                        String ar = common.getString("ar");
                        String isNew = common.getString("is_new");
                        String currDate = DateFormatUtils.format(jsonObject.getLong("ts"), "yyyy-MM-dd");
                        Long duringTime = jsonObject.getJSONObject("page").getLong("during_time");
                        Long ts = jsonObject.getLong("ts");
                        String sid = common.getString("sid");
                        long uv = 0;
                        if (!currDate.equals(isToday.value())) {
                            uv = 1;
                            isToday.update(currDate);
                        }
                        return new TrafficPageViewBean("", "", vc, ch, ar, isNew, "", uv, 0l, 1l, duringTime, ts, sid);
                    }
                })
                .keyBy(v -> v.getSid())
                .map(new RichMapFunction<TrafficPageViewBean, TrafficPageViewBean>() {

                    private ValueState<Boolean> isNewSid;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isNewSid = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isNewSid", Boolean.class));
                    }

                    @Override
                    public TrafficPageViewBean map(TrafficPageViewBean trafficPageViewBean) throws Exception {

                        if (isNewSid.value() == null) {

                            trafficPageViewBean.setSvCt(1l);
                            isNewSid.update(true);
                        } else {
                            trafficPageViewBean.setSvCt(0l);
                        }

                        return trafficPageViewBean;

                    }
                });

    }
}
