package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TradePaymentBean;
import org.lxk.gmall.realtime.bean.TradeProvinceOrderBean;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.AsyncRichDimFunction;
import org.lxk.gmall.realtime.function.DorisMapFunction;
import org.lxk.gmall.realtime.util.DorisUtil;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Dws_10_DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_10_DwsTradeProvinceOrderWindow().start(
                40010,
                2,
                "Dws_10_DwsTradeProvinceOrderWindow",
                GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeProvinceOrderBean> result = stream

                .map(new MapFunction<String, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        TradeProvinceOrderBean bean = TradeProvinceOrderBean.builder()
                                .orderId(jsonObject.getString("order_id"))
                                .provinceId(jsonObject.getString("province_id"))
                                .orderCount(0L)
                                .orderAmount(jsonObject.getBigDecimal("split_total_amount"))
                                .orderDetailId(jsonObject.getString("id"))
                                .ts(jsonObject.getLong("ts") * 1000L)
                                .build();
                        return bean;
                    }

                })
                .keyBy(v -> v.getOrderDetailId())
                .process(new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {

                    private ValueState<TradeProvinceOrderBean> lastBean;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastBean = getRuntimeContext().getState(new ValueStateDescriptor<TradeProvinceOrderBean>("lastBean", TradeProvinceOrderBean.class));
                    }

                    @Override
                    public void processElement(TradeProvinceOrderBean tradeProvinceOrderBean, KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>.Context context, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        TradeProvinceOrderBean lastValue = lastBean.value();
                        if (lastValue == null) {
                            collector.collect(tradeProvinceOrderBean);
                            lastBean.update(tradeProvinceOrderBean);
                        } else {
                            lastValue.setOrderAmount(tradeProvinceOrderBean.getOrderAmount().subtract(lastValue.getOrderAmount()));
                            collector.collect(lastValue);
                            lastBean.update(tradeProvinceOrderBean);
                        }
                    }
                })
                .keyBy(v -> v.getOrderId())
                .process(new KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>() {

                    private ValueState<Boolean> isNewState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isNewState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isNew", Boolean.class));
                    }

                    @Override
                    public void processElement(TradeProvinceOrderBean tradeProvinceOrderBean, KeyedProcessFunction<String, TradeProvinceOrderBean, TradeProvinceOrderBean>.Context context, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        if (isNewState.value() == null) {
                            tradeProvinceOrderBean.setOrderCount(1L);
                            isNewState.update(true);
                        } else {
                            tradeProvinceOrderBean.setOrderCount(0L);
                        }
                        collector.collect(tradeProvinceOrderBean);


                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withIdleness(Duration.ofMinutes(1))
                                .withTimestampAssigner((v, ts) -> v.getTs())
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean v1, TradeProvinceOrderBean v2) throws Exception {
                        v1.setOrderAmount(v1.getOrderAmount().add(v2.getOrderAmount()));
                        v1.setOrderCount(v1.getOrderCount() + v2.getOrderCount());
                        return v1;

                    }
                }, new AllWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        TradeProvinceOrderBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss"));
                        next.setEdt(DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss"));
                        next.setCurDate(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd"));
                        collector.collect(next);
                    }
                });
        SingleOutputStreamOperator<TradeProvinceOrderBean> result1 = AsyncDataStream
                .unorderedWait(result
                        , new AsyncRichDimFunction<TradeProvinceOrderBean>() {
                            @Override
                            public void addDim(TradeProvinceOrderBean bean, JSONObject dimRow) {
                                bean.setProvinceName(dimRow.getString("name"));

                            }

                            @Override
                            public String getRowKey(TradeProvinceOrderBean bean) {
                                return bean.getProvinceId();
                            }

                            @Override
                            public String getTableStr() {
                                return "dim_base_province";
                            }
                        }, GmallConstant.TOW_DAYS, TimeUnit.SECONDS);
        result1
                .print();
                /*.map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink(""));*/

    }
}
