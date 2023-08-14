package org.lxk.gmall.realtime.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Set;
/**
 * Author: Felix
 * Date: 2022/5/14
 * Desc: 交易域品牌-品类-用户粒度退单实体类
 */
@Data
@AllArgsConstructor
@Builder
public class TradeTrademarkCategoryUserRefundBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;
    // 订单 ID
    @JSONField(serialize = false)
    Set<String> orderIdSet;
    // sku_id
    @JSONField(serialize = false)
    String skuId;
    // 用户 ID
    String userId;
    String curDate;
    // 退单次数
    Long refundCount;
    BigDecimal refundAmount;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}