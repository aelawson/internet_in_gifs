package org.aelawson.util;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import edu.stanford.nlp.semgraph.SemanticGraph;

import org.aelawson.util.SemanticSummary;

public class SummarizeSemanticGraph implements FlatMapFunction<SemanticGraph, SemanticSummary> {
    public static final long serialVersionUID = 1L;

    @Override
    public void flatMap(SemanticGraph value, Collector<SemanticSummary> out) {
        ArrayList<String> dependencies = new ArrayList<String>();
        dependencies.add("dependency 1");

        out.collect(new SemanticSummary("subject", dependencies));
    }
}