package org.lxk.gmall.realtime.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.lxk.gmall.realtime.util.IkUtil;

import java.util.Set;
@FunctionHint(output = @DataTypeHint("row<kw string>"))
public class Kasplit extends TableFunction<Row> {
    public void eval(String keywords) {
        if (keywords == null) return;
        Set<String> words = IkUtil.split(keywords);
        for (String word : words) {
            collect(Row.of(word));
        }


    }

}
