package com.example.gmallsuger.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

@Mapper
public interface TradeMapper {
    @Select("select " +
            "sum(order_amount) order_amount " +
            "from dws_trade_sku_order_window " +
            "partition (par#{date})")
BigDecimal gmv(Integer date);

}
