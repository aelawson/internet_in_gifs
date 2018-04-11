package org.aelawson.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class TokenizeTweet implements FlatMapFunction<String, Tuple2<String, Integer>> {
  public static final long serialVersionUID = 1L;
  private transient ObjectMapper jsonParser;

  @Override
  public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
    if (jsonParser == null) {
      jsonParser = new ObjectMapper();
    }

    JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
    if (jsonNode.has("text")) {
      String[] tokens = jsonNode.get("text").asText().split("\\s");
      for (String token : tokens) {
        if (!token.equals("")) {
          out.collect(new Tuple2<>(token, 1));
        }
      }
    }
  }
}