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

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import org.aelawson.util.TwitterExampleData;
import org.aelawson.util.TokenizeTweet;

public class TwitterAnalysis {

	public static void main(String[] args) throws Exception {
		Configurations configs = new Configurations();

		try {
		    PropertiesConfiguration twitterConfig = configs.properties(
						new File("src/main/resources/twitter.properties")
		    );
		}
		catch (ConfigurationException e) {
		    System.out.println("Error loading configuration...");
		    System.exit(1);
		}

		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);

		System.out.println("Executing Twitter analysis with example data.");
		DataStream<String> streamSource = env.fromElements(TwitterExampleData.TEXTS);

		DataStream<Tuple2<String, Integer>> tweets = streamSource
				.flatMap(new TokenizeTweet())
				.keyBy(0)
				.sum(1);

		tweets.print();

		env.execute("Simple Twitter analysis.");
	}
}