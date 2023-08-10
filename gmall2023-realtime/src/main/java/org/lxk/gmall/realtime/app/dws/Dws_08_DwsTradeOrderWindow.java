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
import org.lxk.gmall.realtime.bean.TradeOrderBean;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.DorisMapFunction;
import org.lxk.gmall.realtime.util.DorisUtil;

import java.time.Duration;

public class Dws_08_DwsTradeOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_08_DwsTradeOrderWindow().start(
                40008,
                2,
                "Dws_08_DwsTradeOrderWindow",
                GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        // 1 parse as pojo
        SingleOutputStreamOperator<TradeOrderBean> orderData = parseRawDateStream(stream);
        // 2 agg
        SingleOutputStreamOperator<TradeOrderBean> processedData = aggOrderData(orderData);
        // save
        saveDataToDoris(processedData);
    }

    private void saveDataToDoris(SingleOutputStreamOperator<TradeOrderBean> processedData) {
        processedData
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_trade_order_window"));
    }

    private SingleOutputStreamOperator<TradeOrderBean> aggOrderData(SingleOutputStreamOperator<TradeOrderBean> orderData) {
return        orderData
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofMinutes(1))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new AggregationFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean v1, TradeOrderBean v2) throws Exception {
                        v1.setOrderUniqueUserCount(v1.getOrderUniqueUserCount() + v2.getOrderUniqueUserCount());
                        v1.setOrderNewUserCount(v1.getOrderNewUserCount() + v2.getOrderNewUserCount());
                        return v1;
                    }
                },new AllWindowFunction<TradeOrderBean,TradeOrderBean, TimeWindow>(){
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> iterable, Collector<TradeOrderBean> collector) throws Exception {
                        TradeOrderBean bean = iterable.iterator().next();
                        bean.setStt(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss"));
                        bean.setEdt(DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss"));
                        bean.setCurDate(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd"));
                        collector.collect(bean);
                    }
                });
    }

    private SingleOutputStreamOperator<TradeOrderBean> parseRawDateStream(DataStreamSource<String> stream) {
      return  stream
                .map(JSON::parseObject)
                .keyBy(json -> json.getString("order_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    private ValueState<String> isTodayState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isTodayState = getRuntimeContext().getState(new ValueStateDescriptor<String>("isToday", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeOrderBean>.Context context, Collector<TradeOrderBean> collector) throws Exception {
                        long ts = jsonObject.getLong("ts") * 1000L;
                        String isToday = isTodayState.value();
                        String today = DateFormatUtils.format(ts, "yyyy-MM-dd");
                        TradeOrderBean bean = new TradeOrderBean("", "", "", 0L, 0L, ts);
                        if (!today.equals(isToday)) {
                            bean.setOrderUniqueUserCount(1L);
                            if (null == isToday) {
                                bean.setOrderNewUserCount(1L);
                            }
                            isTodayState.update(today);
                            collector.collect(bean);
                        }
                    }

                });
    }
}
