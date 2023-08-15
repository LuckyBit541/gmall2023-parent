package com.example.gmallsuger.service.imp;

import com.alibaba.fastjson.JSONObject;
import com.example.gmallsuger.mapper.TradeMapper;
import com.example.gmallsuger.service.TradeService;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class TradeServiceImp implements TradeService {
    @Autowired
    TradeMapper tradeMapper;
    @Override
    public String gmv(Integer date) {
        BigDecimal gmv = tradeMapper.gmv(date);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("status",0);
        jsonObject.put("msg","");
        jsonObject.put("data",gmv.intValue());
        return jsonObject.toString();
    }
}
