package org.lxk.gmall.realtime.function;

import com.alibaba.fastjson.JSONObject;

public interface DimFunction<T> {
    public abstract void addDim(T bean, JSONObject dimRow);

    public abstract String getRowKey(T bean);

    public abstract String getTableStr() ;
}
