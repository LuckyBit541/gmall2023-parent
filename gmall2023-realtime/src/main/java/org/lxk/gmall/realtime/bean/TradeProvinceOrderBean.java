package org.lxk.gmall.realtime.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;

/**
 * @author Felix
 * @date 2022/12/27
 * Desc: 交易域省份粒度下单实体类
 */
@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 省份 ID
    String provinceId;
    // 省份名称
    @Builder.Default
    String provinceName = "";
    // 订单 ID
@JSONField(serialize = false)
    String orderId;
String curDate;
    // 累计下单次数
    Long orderCount;
    // 累计下单金额
    BigDecimal orderAmount;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
    String orderDetailId;
}