package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import org.lxk.gmall.realtime.common.GmallConstant;
import org.lxk.gmall.realtime.function.AsyncRichDimFunction;
import org.lxk.gmall.realtime.function.DorisMapFunction;

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



    }
}
