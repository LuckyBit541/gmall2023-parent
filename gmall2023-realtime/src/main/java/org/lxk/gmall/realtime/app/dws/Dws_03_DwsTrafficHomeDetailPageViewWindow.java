package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.DorisMapFunction;
import org.lxk.gmall.realtime.util.DorisUtil;

import java.time.Duration;

public class Dws_03_DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_03_DwsTrafficHomeDetailPageViewWindow().start(40003,2, "Dws_03_DwsTrafficHomeDetailPageViewWindow", GmallConstant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        // 1 过滤数据
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> rawData = filterPage(stream);
        // 2 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processedDataStream = aggData(rawData);
        // 3 写入doris
        writeToDoris(processedDataStream);

    }

    private void writeToDoris(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processedDataStream) {
        processedDataStream
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_traffic_home_detail_page_view_window"));

    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> aggData(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> rawData) {
      return  rawData
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean bean1, TrafficHomeDetailPageViewBean bean2) throws Exception {
                        bean1.setGoodDetailUvCt(bean1.getGoodDetailUvCt()+ bean2.getGoodDetailUvCt());
                        bean1.setHomeUvCt(bean1.getHomeUvCt()+ bean2.getHomeUvCt());
                        return bean1;

                    }

                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow
                        >() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        TrafficHomeDetailPageViewBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtils.format(window.getStart(),"yyyy-MM-dd HH:ss:mm"));
                        next.setEdt(DateFormatUtils.format(window.getEnd(),"yyyy-MM-dd HH:ss:mm"));
                        next.setCurDate(DateFormatUtils.format(window.getStart(),"yyyy-MM-dd"));
                        collector.collect(next);
                    }
                });


    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> filterPage(DataStreamSource<String> stream) {
       return stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean,ts)-> bean.getLong("ts"))
                                .withIdleness(Duration.ofMinutes(1))
                ).filter(new FilterFunction<JSONObject>() {
                   @Override
                   public boolean filter(JSONObject jsonObject) throws Exception {

                       String pageId = jsonObject.getJSONObject("page").getString("page_id");
                       return "good_detail".equals(pageId) || "home".equals(pageId);

                   }
               })
               .keyBy(v->v.getJSONObject("common").getString("mid"))
                .flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {


                    private ValueState<String> home;
                    private ValueState<String> detail;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        detail = getRuntimeContext().getState(new ValueStateDescriptor<String>("detail", String.class));
                        home = getRuntimeContext().getState(new ValueStateDescriptor<String>("home", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        String today = DateFormatUtils.format(jsonObject.getLong("ts"), "yyyy-MM-dd");

                          JSONObject page = jsonObject.getJSONObject("page");
                        TrafficHomeDetailPageViewBean bean = new TrafficHomeDetailPageViewBean("","","",0l,0l,jsonObject.getLong("ts"));
                        String pageId = page.getString("page_id");
                        if ("home".equals(pageId)){

                            if (!today.equals(home.value())){
                                bean.setHomeUvCt(1l);
                                home.update(today);
                            }
                        }else {

                            if (!today.equals(detail.value())){
                                bean.setGoodDetailUvCt(1l);
                                detail.update(today);
                            }
                        }
                        if (bean.getGoodDetailUvCt()+ bean.getHomeUvCt()==1){
                            collector.collect(bean);
                        }



                    }
                });
    }
}
