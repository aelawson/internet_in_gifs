/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.aelawson;

import java.io.File;
import java.util.Properties;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FoldFunction;

import org.aelawson.util.TokenTag;
import org.aelawson.util.KeyByTag;
import org.aelawson.util.TagAndTokenize;
import org.aelawson.util.FilterTokenTags;
import org.aelawson.util.FoldTokenTags;

public class InternetInGifs {

  public static void main(String[] args) throws Exception {
    PropertiesConfiguration twitterConfig = null;
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final Logger LOG = LoggerFactory.getLogger(InternetInGifs.class);

    Configurations configs = new Configurations();

    try {
        twitterConfig = configs.properties(
            new File("src/main/resources/twitter.properties")
        );
    }
    catch (ConfigurationException e) {
        LOG.info("Error loading configuration...");
        System.exit(1);
    }

    Properties twitterProps = new Properties();
    twitterProps.setProperty(TwitterSource.CONSUMER_KEY, twitterConfig.getString("key"));
    twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, twitterConfig.getString("secret"));
    twitterProps.setProperty(TwitterSource.TOKEN, twitterConfig.getString("token"));
    twitterProps.setProperty(TwitterSource.TOKEN_SECRET, twitterConfig.getString("token_secret"));

    TwitterSource twitterSource = new TwitterSource(twitterProps);

    LOG.info("Executing Twitter analysis with example data.");
    DataStream<String> streamSource = env.addSource(twitterSource);

    DataStream<TokenTag> tokenTags = streamSource.flatMap(new TagAndTokenize());
    KeyedStream<TokenTag, String> keyedTokenTags = tokenTags.filter(new FilterTokenTags())
        .keyBy(new KeyByTag());

    DataStream<Tuple2<String, String>> result = keyedTokenTags.timeWindow(Time.seconds(10))
        .fold(new Tuple2<String, String>("", ""), new FoldTokenTags());

    result.print();

    env.execute("Simple Twitter analysis.");
  }
}