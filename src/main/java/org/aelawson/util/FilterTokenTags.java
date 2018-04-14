package org.aelawson.util;

import org.aelawson.util.TokenTag;

import org.apache.flink.api.common.functions.FilterFunction;

import java.util.regex.Pattern;

public class FilterTokenTags implements FilterFunction<TokenTag> {
    public static final long serialVersionUID = 1L;

    @Override
    public boolean filter(TokenTag tokenTag) throws Exception {
        return this.isNoun(tokenTag) || this.isVerb(tokenTag) || this.isAdj(tokenTag);
    }

    private boolean isNoun(TokenTag tokenTag) {
        return Pattern.matches("NN[A-Z]?", tokenTag.tag);
    }

    private boolean isVerb(TokenTag tokenTag) {
        return Pattern.matches("VV[A-Z]?", tokenTag.tag);
    }

    private boolean isAdj(TokenTag tokenTag) {
        return Pattern.matches("JJ[A-Z]?", tokenTag.tag);
    }
}