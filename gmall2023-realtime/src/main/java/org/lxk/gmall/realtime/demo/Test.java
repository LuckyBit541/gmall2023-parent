package org.lxk.gmall.realtime.demo;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

public class Test {
    public static void main(String[] args) {
        Date date = new Date();
        System.out.println(DateFormatUtils.format(date.getTime()/1000, "yyyy-MM-dd HH:mm:ss"));
        System.out.println(date.getTime());

    }
}
