package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
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
import org.lxk.gmall.realtime.bean.UserLoginBean;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.DorisMapFunction;
import org.lxk.gmall.realtime.util.DorisUtil;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class Dws_04_DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_04_DwsUserUserLoginWindow().start(40004, 2, "Dws_04_DwsUserUserLoginWindow", GmallConstant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        // 1 过滤登录数据
        SingleOutputStreamOperator<JSONObject> filteredData = filterLoginData(stream);
        // 2 parse data as pojo
        SingleOutputStreamOperator<UserLoginBean> parsedData = parseAndProcess(filteredData);
        // 3 agg
        SingleOutputStreamOperator<UserLoginBean> finalData = aggData(parsedData);
        // 4 save
        saveDataToDoris(finalData);

    }

    private void saveDataToDoris(SingleOutputStreamOperator<UserLoginBean> finalData) {
        finalData
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_user_user_login_window"));

    }

    private SingleOutputStreamOperator<UserLoginBean> aggData(SingleOutputStreamOperator<UserLoginBean> parsedData) {

        return parsedData
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<UserLoginBean>() {

                    @Override
                    public UserLoginBean reduce(UserLoginBean v1, UserLoginBean v2) throws Exception {
                        v1.setUuCt(v1.getUuCt()+v2.getUuCt());
                        v1.setBackCt(v1.getBackCt()+v2.getBackCt());
                        return v1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        UserLoginBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtils.format(window.getStart(),"yyyy-MM-dd HH:mm:ss"));
                        next.setEdt(DateFormatUtils.format(window.getEnd(),"yyyy-MM-dd HH:mm:ss"));
                        next.setCurDate(DateFormatUtils.format(window.getStart(),"yyyy-MM-dd"));
                        collector.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<UserLoginBean> parseAndProcess(SingleOutputStreamOperator<JSONObject> filteredData) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return filteredData
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((t, v) -> t.getLong("ts"))
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(v->v.getJSONObject("common").getString("uid"))
                .map(new RichMapFunction<JSONObject, UserLoginBean>() {

                    private ValueState<String> lastLonginDataState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastLonginDataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDate", String.class));
                    }

                    @Override

                    public UserLoginBean map(JSONObject jsonObject) throws Exception {
                        Long ts = jsonObject.getLong("ts");
                        String today = DateFormatUtils.format(ts, "yyyy-MM-dd");
                        UserLoginBean bean = new UserLoginBean("", "", "", 0l, 0l, ts);
                        String lastDate = lastLonginDataState.value();
                        if (!today.equals(lastDate)) {
                            bean.setUuCt(1l);
                            if (lastDate != null && ts - simpleDateFormat.parse(lastDate).getTime() > GmallConstant.SEVEN_DAYS) {
                                bean.setBackCt(1l);
                            }
                            lastLonginDataState.update(today);

                        }
                        return bean;

                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> filterLoginData(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        String uid = jsonObject.getJSONObject("common").getString("uid");
                        return ("login".equals(lastPageId) || null == lastPageId) && null != uid;
                    }
                });
    }
}
