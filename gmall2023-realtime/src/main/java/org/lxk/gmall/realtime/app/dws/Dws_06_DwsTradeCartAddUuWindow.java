package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.CartAddUuBean;
import org.lxk.gmall.realtime.bean.UserRegisterBean;
import org.lxk.gmall.realtime.function.DorisMapFunction;
import org.lxk.gmall.realtime.util.DorisUtil;

import javax.sql.rowset.CachedRowSet;
import java.time.Duration;

public class Dws_06_DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_06_DwsTradeCartAddUuWindow().start(
                40006,
                2,
                "Dws_06_DwsTradeCartAddUuWindow",
                "dwd_trade_cart_add"
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment executionEnvironment, DataStreamSource<String> stream) {
        // 1 parse as pojo
        stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((v, t) -> v.getLong("ts")*1000L)
                                .withIdleness(Duration.ofSeconds(60))
                )
                .keyBy(v->v.getString("user_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

                    private ValueState<String> lastAddDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        lastAddDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAddDate", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject jsonObject, Collector<CartAddUuBean> collector) throws Exception {
                        Long ts = jsonObject.getLong("ts")*1000L;
                        String today = DateFormatUtils.format(ts, "yyyy-MM-dd");
                        CartAddUuBean cartAddUuBean = new CartAddUuBean("","","",0L,ts);
                        if (!today.equals(lastAddDateState.value())) {
                          cartAddUuBean.setCartAddUuCt(1L);
                          lastAddDateState.update(today);
                          collector.collect(cartAddUuBean);
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean v1, CartAddUuBean v2) throws Exception {
                        v1.setCartAddUuCt(v1.getCartAddUuCt()+v2.getCartAddUuCt());
                        return v1;
                    }
                },new AllWindowFunction<CartAddUuBean,CartAddUuBean, TimeWindow>(){
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        CartAddUuBean next = iterable.iterator().next();
                        next.setStt(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss"));
                        next.setEdt(DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss"));
                        next.setCurDate(DateFormatUtils.format(window.getStart(), "yyyy-MM-dd"));
                        collector.collect(next);
                    }
                })
                .map(new DorisMapFunction<CartAddUuBean>())

                .sinkTo(DorisUtil.getDorisSink("gmall2023.dws_trade_cart_add_uu_window"));







        // 2 aggregate
        // 3 save to doris
    }
}
