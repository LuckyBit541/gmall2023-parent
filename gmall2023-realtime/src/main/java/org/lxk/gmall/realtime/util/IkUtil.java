package org.lxk.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

public class IkUtil {
    public static Set<String> split(String keywords) {
        Set<String> strings = new HashSet<>();
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keywords), true);
        try {
            Lexeme next = ikSegmenter.next();
            while (next !=null){
                String lexemeText = next.getLexemeText();
                strings.add(lexemeText);
                next=ikSegmenter.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
return strings;

    }
}
