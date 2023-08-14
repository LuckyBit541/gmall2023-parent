package org.lxk.gmall.realtime.demo;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class Test {

    public static void main(String[] args) {
        CompletableFuture
                .supplyAsync(new Supplier<String>() {
                    @Override
                    public String get() {
                        System.out.println("1111111111");
                        return "111111111111111";
                    }
                })
                .thenApplyAsync(new Function<String, String>() {
                    @Override
                    public String apply(String s) {
                        System.out.println("22222222222");
                       return "22222" ;
                    }
                })
                .thenAccept(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        System.out.println(s);

                    }
                });


    }
}
