package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TradeSkuOrderBean;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.addDimsFunction;

import java.math.BigDecimal;
import java.time.Duration;

public class Dws_09_DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow().start(
                40009,
                2,
                "Dws_09_DwsTradeSkuOrderWindow",
                GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        // 1 parse as pojo
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseRawStream(stream);
        // 2 agg
        SingleOutputStreamOperator<TradeSkuOrderBean> beanSteam = aggBeanSteam(beanStream);
       // beanSteam.print();
        // 3 add dims


        SingleOutputStreamOperator<TradeSkuOrderBean> joinedDataSteam = addDim(beanSteam);
        joinedDataSteam.print();


    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> addDim(SingleOutputStreamOperator<TradeSkuOrderBean> beanSteam) {
return    beanSteam
            .map(new addDimsFunction<TradeSkuOrderBean>(){
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dimRow) {
                    //System.out.printf("::::::::::::::::::::::::::::::::::::::::"+bean+"==="+dimRow);
                    bean.setSkuName(dimRow.getString("sku_name"));
                    bean.setSpuId(dimRow.getString("spu_id"));
                    bean.setCategory3Id(dimRow.getString("category3_id"));
                    bean.setTrademarkId(dimRow.getString("tm_id"));
                }

                @Override
                public String getRowKey(TradeSkuOrderBean bean) {
                    return bean.getSkuId();
                }

                @Override
                public String getTableStr() {
                    return "dim_sku_info";
                }
            })
            .map(new addDimsFunction<TradeSkuOrderBean>(){
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dimRow) {
                    bean.setSpuName(dimRow.getString("spu_name"));
                }

                @Override
                public String getRowKey(TradeSkuOrderBean bean) {
                    return bean.getSpuId();
                }

                @Override
                public String getTableStr() {
                    return "dim_spu_info";
                }
            })
            .map(new addDimsFunction<TradeSkuOrderBean>(){
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dimRow) {
                    bean.setTrademarkName(dimRow.getString("tm_name"));

                }

                @Override
                public String getRowKey(TradeSkuOrderBean bean) {
                    return bean.getTrademarkId();
                }

                @Override
                public String getTableStr() {
                    return "dim_base_trademark";
                }
            })
            .map(new addDimsFunction<TradeSkuOrderBean>(){
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dimRow) {
                    bean.setCategory3Name(dimRow.getString("name"));
                    bean.setCategory2Id(dimRow.getString("category2_id"));

                }

                @Override
                public String getRowKey(TradeSkuOrderBean bean) {
                    return bean.getCategory3Id();
                }

                @Override
                public String getTableStr() {
                    return "dim_base_category3";
                }
            })
            .map(new addDimsFunction<TradeSkuOrderBean>(){
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dimRow) {
                    bean.setCategory2Name(dimRow.getString("name"));
                    bean.setCategory1Id(dimRow.getString("category1_id"));
                }

                @Override
                public String getRowKey(TradeSkuOrderBean bean) {
                    return bean.getCategory2Id();
                }

                @Override
                public String getTableStr() {
                    return "dim_base_category2" ;
                }
            })
            .map(new addDimsFunction<TradeSkuOrderBean>(){
                @Override
                public void addDim(TradeSkuOrderBean bean, JSONObject dimRow) {
                    bean.setCategory1Name(dimRow.getString("name"));
                }

                @Override
                public String getRowKey(TradeSkuOrderBean bean) {
                    return bean.getCategory1Id();
                }

                @Override
                public String getTableStr() {
                    return "dim_base_category1";
                }
            });

    }


    private SingleOutputStreamOperator<TradeSkuOrderBean> aggBeanSteam(SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(60)
                                ))
                .keyBy(bean -> bean.getId())
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(new AggregationFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean v1, TradeSkuOrderBean v2) throws Exception {
                        v1.setOriginalAmount(v1.getOriginalAmount().add(v2.getOriginalAmount()));
                        v1.setActivityAmount(v1.getActivityAmount().add(v2.getActivityAmount()));
                        v1.setOrderAmount(v1.getOrderAmount().add(v2.getOrderAmount()));
                        v1.setCouponAmount(v1.getCouponAmount().add(v2.getCouponAmount()));
                        return v1;
                    }
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                        TradeSkuOrderBean next = iterable.iterator().next();
                        TimeWindow window = context.window();
                        next.setStt(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss"));
                        next.setEdt(DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss"));
                        next.setCurDate(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd"));
                        collector.collect(next);

                    }
                });

    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> parseRawStream(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("id"))
                .map(new RichMapFunction<JSONObject, TradeSkuOrderBean>() {


                    private ValueState<TradeSkuOrderBean> lastBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<TradeSkuOrderBean> desc = new ValueStateDescriptor<>("flag", TradeSkuOrderBean.class);
                        StateTtlConfig ttlconfig = StateTtlConfig.newBuilder(Time.seconds(5))
                                .updateTtlOnReadAndWrite()
                                .neverReturnExpired()
                                .build();
                        desc.enableTimeToLive(ttlconfig);

                        lastBeanState = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObject) throws Exception {
                        long ts = jsonObject.getLong("ts") * 1000L;
                        BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
                        BigDecimal splitActivityAmount = jsonObject.getBigDecimal("split_activity_amount");
                        BigDecimal splitCouponAmount = jsonObject.getBigDecimal("split_coupon_amount");
                        BigDecimal splitOriginalAmount = jsonObject.getBigDecimal("split_original_amount");
                        String id = jsonObject.getString("id");
                        String skuId = jsonObject.getString("sku_id");
                        TradeSkuOrderBean bean = TradeSkuOrderBean
                                .builder()
                                .id(id)
                                .skuId(skuId)
                                .ts(ts)
                                .originalAmount(splitOriginalAmount)
                                .activityAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .couponAmount(splitCouponAmount)
                                .build();
                        TradeSkuOrderBean lastBean = lastBeanState.value();
                        if (lastBean != null) {
                            lastBean.setOriginalAmount(bean.getOriginalAmount().subtract(lastBean.getOriginalAmount()));
                            lastBean.setActivityAmount(bean.getActivityAmount().subtract(lastBean.getActivityAmount()));
                            lastBean.setOrderAmount(bean.getOrderAmount().subtract(lastBean.getOrderAmount()));
                            lastBean.setCouponAmount(bean.getCouponAmount().subtract(lastBean.getCouponAmount()));
                        } else {
                            lastBeanState.update(bean);
                        }
                        return bean;


                    }
                });
    }
}
