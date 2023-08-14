package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TradeProvinceOrderBean;
import org.lxk.gmall.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.AsyncRichDimFunction;
import org.lxk.gmall.realtime.function.DorisMapFunction;
import org.lxk.gmall.realtime.util.DorisUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class Dws_11_DwsTradeTrademarkCategoryUserRefundWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_11_DwsTradeTrademarkCategoryUserRefundWindow().start(
                40011,
                2,
                "Dws_11_DwsTradeTrademarkCategoryUserRefundWindow",
                GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> skuIdSteam = stream
                .map(JSON::parseObject)
                .map(new MapFunction<JSONObject, TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean map(JSONObject jsonObject) throws Exception {
                        HashSet<String> orderSet = new HashSet<>();
                        orderSet.add(jsonObject.getString("order_id"));
                        TradeTrademarkCategoryUserRefundBean bean = TradeTrademarkCategoryUserRefundBean
                                .builder()
                                .skuId(jsonObject.getString("sku_id"))
                                .userId(jsonObject.getString("user_id"))
                                .refundAmount(jsonObject.getBigDecimal("refund_amount"))
                                .refundCount(0L)
                                .ts(jsonObject.getLong("ts") * 1000L)
                                .orderIdSet(orderSet)
                                .build();
                        return bean;
                    }
                });
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> cate3Stream = AsyncDataStream
                .unorderedWait(
                        skuIdSteam,
                        new AsyncRichDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public void addDim(TradeTrademarkCategoryUserRefundBean bean, JSONObject dimRow) {
                                bean.setTrademarkId(dimRow.getString("tm_id"));
                                bean.setCategory3Id(dimRow.getString("category3_id"));

                            }

                            @Override
                            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                                return bean.getSkuId();
                            }

                            @Override
                            public String getTableStr() {
                                return "dim_sku_info";
                            }
                        },
                        GmallConstant.TOW_DAYS,
                        TimeUnit.SECONDS
                );
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> lackDimStream = cate3Stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((v, ts) -> v.getTs())
                                .withIdleness(Duration.ofSeconds(60))

                )
                .keyBy(v -> v.getUserId() + v.getCategory3Id() + v.getTrademarkId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean v1, TradeTrademarkCategoryUserRefundBean v2) throws Exception {
                        v1.setRefundAmount(v1.getRefundAmount().add(v2.getRefundAmount()));
                        v1.getOrderIdSet().addAll(v2.getOrderIdSet());
                        return v1;

                    }
                }, new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>.Context context, Iterable<TradeTrademarkCategoryUserRefundBean> elements, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                        TimeWindow window = context.window();
                        TradeTrademarkCategoryUserRefundBean next = elements.iterator().next();
                        next.setStt(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss"));
                        next.setEdt(DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss"));
                        next.setCurDate(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd"));
                        out.collect(next);

                    }
                });
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmStream = AsyncDataStream
                .unorderedWait(lackDimStream,
                        new AsyncRichDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public String getTableStr() {
                                return "dim_base_trademark";
                            }

                            @Override
                            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                                return bean.getTrademarkId();
                            }

                            @Override
                            public void addDim(TradeTrademarkCategoryUserRefundBean bean,
                                               JSONObject dim) {
                                bean.setTrademarkName(dim.getString("tm_name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream
                .unorderedWait(tmStream,
                        new AsyncRichDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public String getTableStr() {
                                return "dim_base_category3";
                            }

                            @Override
                            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                                return bean.getCategory3Id();
                            }

                            @Override
                            public void addDim(TradeTrademarkCategoryUserRefundBean bean,
                                               JSONObject dim) {
                                bean.setCategory3Name(dim.getString("name"));
                                bean.setCategory2Id(dim.getString("category2_id"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c2Stream = AsyncDataStream
                .unorderedWait(c3Stream,
                        new AsyncRichDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public String getTableStr() {
                                return "dim_base_category2";
                            }

                            @Override
                            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                                return bean.getCategory2Id();
                            }

                            @Override
                            public void addDim(TradeTrademarkCategoryUserRefundBean bean,
                                               JSONObject dim) {
                                bean.setCategory2Name(dim.getString("name"));
                                bean.setCategory1Id(dim.getString("category1_id"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );

        AsyncDataStream
                .unorderedWait(c2Stream,
                        new AsyncRichDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public String getTableStr() {
                                return "dim_base_category1";
                            }

                            @Override
                            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                                return bean.getCategory1Id();
                            }

                            @Override
                            public void addDim(TradeTrademarkCategoryUserRefundBean bean,
                                               JSONObject dim) {
                                bean.setCategory1Name(dim.getString("name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                )
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_trade_trademark_category_user_refund_window"));



    }
}
