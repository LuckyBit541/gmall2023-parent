package com.example.gmallsuger.controller;

import com.example.gmallsuger.service.imp.TradeServiceImp;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
public class SugerController {
    @Autowired
    TradeServiceImp tradeServiceImp;

    @RequestMapping("/suger/trade/gmv")
    public String gmv(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = Integer.valueOf(new SimpleDateFormat("yyyyMMdd").format(new Date()));

        }
        return tradeServiceImp.gmv(date);
    }
}
