package org.aelawson.util;

import org.aelawson.util.TokenTag;

import org.apache.flink.api.java.functions.KeySelector;

public class KeyByTag implements KeySelector<TokenTag, String> {
    public static final long serialVersionUID = 1L;

    @Override
    public String getKey(TokenTag tokenTag) {
      return tokenTag.tag;
    }
}