package org.aelawson.util;

import org.aelawson.util.TokenTag;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FoldTokenTags implements FoldFunction<TokenTag, Tuple2<String, String>> {
    public static final long serialVersionUID = 1L;

    @Override
    public Tuple2<String, String> fold(Tuple2<String, String> byTag, TokenTag tokenTag) throws Exception {
        byTag.f0 = tokenTag.tag;
        byTag.f1 = byTag.f1 + "|" + tokenTag.token;

        return byTag;
    }
}