package org.lxk.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.val;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.network.ChannelState;
import org.lxk.gmall.realtime.app.BaseApp;
import org.lxk.gmall.realtime.bean.TrafficPageViewBean;
import org.lxk.gmall.realtime.common.GmallConstant;

public class Dws_02_DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new Dws_02_DwsTrafficVcChArIsNewPageViewWindow().start(40002,2 ,"Dws_02_DwsTrafficVcChArIsNewPageViewWindow" , GmallConstant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> dataStream) {
        // 1 parse json as  pojo
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageView = parseStringToPojo(dataStream);
        // 2 aggregate
        countUvSvPv(trafficPageView);
        // 3 save data to doris



    }

    private void countUvSvPv(SingleOutputStreamOperator<TrafficPageViewBean> trafficPageView) {
        trafficPageView.map(new RichMapFunction<TrafficPageViewBean, TrafficPageViewBean>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public TrafficPageViewBean map(TrafficPageViewBean trafficPageViewBean) throws Exception {
                return null;
            }
        });

    }

    private SingleOutputStreamOperator<TrafficPageViewBean> parseStringToPojo(DataStreamSource<String> dataStream) {
        dataStream
                .keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                String mid = JSON.parseObject(value).getJSONObject("common").getString("mid");

                return mid;
            }
        })
                .map(new RichMapFunction<String, TrafficPageViewBean>() {

                    private ValueState<String> isToday;

                    @Override
            public void open(Configuration parameters) throws Exception {

                        isToday = getRuntimeContext().getState(new ValueStateDescriptor<String>("isToday", String.class));
            }

            @Override
            public TrafficPageViewBean map(String json) throws Exception {
                JSONObject jsonObject = JSON.parseObject(json);
                JSONObject common = jsonObject.getJSONObject("common");
                String ch = common.getString("ch");
                String vc = common.getString("vc");
                String ar = common.getString("ar");
                String isNew = common.getString("is_new");
                String currDate = DateFormatUtils.format(jsonObject.getLong("ts"), "yyyy-MM-dd");
                Long duringTime = jsonObject.getJSONObject("page").getLong("during_time");
                Long ts = jsonObject.getLong("ts");
                String sid = common.getString("sid");
                long uv=0;
                if (!currDate.equals(isToday.value())){
                   uv=1;
                   isToday.update(currDate);
                }
                return new TrafficPageViewBean("","",vc,ch,ar,isNew,currDate,uv,0l,1l,duringTime,ts,sid);
            }
        })
                .keyBy(v->v.getSid())
                .map(new RichMapFunction<TrafficPageViewBean, TrafficPageViewBean>() {

                    private ValueState<String> isNewSid;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isNewSid = getRuntimeContext().getState(new ValueStateDescriptor<String>("isNewSid", String.class));
                    }

                    @Override
                    public TrafficPageViewBean map(TrafficPageViewBean trafficPageViewBean) throws Exception {
                        if (isNewSid.value() != null) {

                        }
                        return null;
                    }
                });
        return null;
    }
}
