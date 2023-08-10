package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TradePaymentBean;
import org.lxk.gmall.realtime.function.DorisMapFunction;
import org.lxk.gmall.realtime.util.DorisUtil;

import java.time.Duration;

public class Dws_07_DwsTradePaymentSucWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_07_DwsTradePaymentSucWindow().start(
                40007,
                2,
                "Dws_07_DwsTradePaymentSucWindow",
                "dwd_trade_pay_detail_suc"
        );
    }
    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        // 1 解析处理数据
        SingleOutputStreamOperator<TradePaymentBean> parsedDataSteam = parseRawDateStream(stream);

        // 2 开窗聚合
        SingleOutputStreamOperator<TradePaymentBean> aggedDataStream = aggDataSteam(parsedDataSteam);
        // 3 写入doris
        saveDataToDoris(aggedDataStream);
    }

    private void saveDataToDoris(SingleOutputStreamOperator<TradePaymentBean> aggedDataStream) {
        aggedDataStream
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_trade_payment_success_uu_window"));
    }

    private SingleOutputStreamOperator<TradePaymentBean> aggDataSteam(SingleOutputStreamOperator<TradePaymentBean> parsedDataSteam) {
    return    parsedDataSteam
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradePaymentBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean,ts)->bean.getTs())
                                .withIdleness(Duration.ofSeconds(60))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new AggregationFunction<TradePaymentBean>() {
                    @Override
                    public TradePaymentBean reduce(TradePaymentBean v1, TradePaymentBean v2) throws Exception {
                        v1.setPaymentSucUniqueUserCount(v1.getPaymentSucUniqueUserCount() + v2.getPaymentSucUniqueUserCount());
                        v1.setPaymentSucNewUserCount(v1.getPaymentSucNewUserCount() + v2.getPaymentSucNewUserCount());
                        return v1;

                    }
                }, new AllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentBean> iterable, Collector<TradePaymentBean> collector) throws Exception {
                        TradePaymentBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss"));
                        next.setEdt(DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss"));
                        next.setCurDate(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd"));
                        collector.collect(next);
                    }
                });
    }

    private SingleOutputStreamOperator<TradePaymentBean> parseRawDateStream(DataStreamSource<String> stream) {
   return     stream
                .map(JSON::parseObject)
                .keyBy(v->v.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {

                    private ValueState<String> isTodayState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isTodayState = getRuntimeContext().getState(new ValueStateDescriptor<String>("isToday", String.class));

                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradePaymentBean>.Context context, Collector<TradePaymentBean> collector) throws Exception {
                        long ts = jsonObject.getLong("ts") * 1000L;
                        String today = DateFormatUtils.format(ts, "yyyy-MM-dd");
                        TradePaymentBean bean = new TradePaymentBean("","","",0L,0L,ts);
                        if (!today.equals(isTodayState.value())) {
                            bean.setPaymentSucUniqueUserCount(1L);
                            if (null==isTodayState.value()){
                                bean.setPaymentSucNewUserCount(1L);
                            }
                            isTodayState.update(today);
                            collector.collect(bean);
                        }
                    }
                });
                
    }
}
