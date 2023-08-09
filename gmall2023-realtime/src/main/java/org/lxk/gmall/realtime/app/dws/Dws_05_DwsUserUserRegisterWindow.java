package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.UserRegisterBean;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.DorisMapFunction;
import org.lxk.gmall.realtime.util.DorisUtil;

import java.time.Duration;

public class Dws_05_DwsUserUserRegisterWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_05_DwsUserUserRegisterWindow().start(40005,2,"Dws_05_DwsUserUserRegisterWindow", GmallConstant.TOPIC_DWD_USER_REGISTER);
    }
    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        // 1 parse as pojo
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((v,t)->v.getLong("create_time"))
                                .withIdleness(Duration.ofSeconds(60))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<JSONObject, Long,UserRegisterBean >() {
                    @Override
                    public Long createAccumulator() {
                        return 0l;
                    }

                    @Override
                    public Long add(JSONObject jsonObject, Long acc) {
                        return acc+1;
                    }

                    @Override
                    public UserRegisterBean getResult(Long aLong) {
                        return new UserRegisterBean("","","",aLong,System.currentTimeMillis());
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return null;
                    }
                },new AllWindowFunction<UserRegisterBean,UserRegisterBean, TimeWindow>(){
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                        UserRegisterBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss"));
                        next.setEdt(DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss"));
                        next.setCurDate(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd"));
                        collector.collect(next);
                    }
                })
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_user_user_register_window"));
                
        // 2 agg

        // 3 save

    }


}
