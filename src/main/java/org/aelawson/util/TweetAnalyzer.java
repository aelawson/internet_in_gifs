package org.aelawson.util;

import java.util.List;

import org.aelawson.util.NLPParser;

import edu.stanford.nlp.ling.CoreLabel;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class TweetAnalyzer implements FlatMapFunction<String, Tuple2<String, Integer>> {
  public static final long serialVersionUID = 1L;
  private transient ObjectMapper jsonParser;
  private transient NLPParser nlpParser;

  @Override
  public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
    jsonParser = jsonParser == null ? new ObjectMapper() : jsonParser;
    nlpParser = nlpParser == null ? new NLPParser() : nlpParser;

    JsonNode jsonNode = this.jsonParser.readValue(value, JsonNode.class);

    if (jsonNode.has("text")) {
      String text = jsonNode.get("text").asText();
      List<CoreLabel> tokens = this.nlpParser.parse(text);
      for (CoreLabel token : tokens) {
        out.collect(new Tuple2<>(token.value(), 1));
      }
    }
  }
}