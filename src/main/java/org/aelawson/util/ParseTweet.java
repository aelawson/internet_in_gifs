package org.aelawson.util;

import java.util.List;

import org.aelawson.util.TokenTag;
import org.aelawson.util.NLPParser;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.semgraph.SemanticGraph;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class ParseTweet implements FlatMapFunction<String, SemanticGraph> {
    public static final long serialVersionUID = 1L;
    private transient ObjectMapper jsonParser;
    private transient NLPParser nlpParser;

    @Override
    public void flatMap(String value, Collector<SemanticGraph> out) throws Exception {
        jsonParser = jsonParser == null ? new ObjectMapper() : jsonParser;
        nlpParser = nlpParser == null ? new NLPParser() : nlpParser;

        JsonNode tweet = this.jsonParser.readValue(value, JsonNode.class);

        if (tweet.has("text") && this.isEnglish(tweet)) {
            String text = tweet.get("text").asText();

            List<SemanticGraph> graphs = this.nlpParser.parse(text);
            graphs.stream()
                .forEach(g -> out.collect(g));
        }
    }

    private boolean isEnglish(JsonNode tweet) {
        return tweet.has("user")
          && tweet.get("user").has("lang")
          && tweet.get("user").get("lang").asText().equals("en");
    }
}