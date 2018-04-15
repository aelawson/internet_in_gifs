package org.aelawson.util;

import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.ie.util.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.semgraph.*;
import edu.stanford.nlp.trees.*;

public class NLPParser {
    private Properties props;
    private StanfordCoreNLP pipeline;
    private Logger logger;

    public NLPParser() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,depparse");
        props.setProperty("coref.algorithm", "neural");

        this.props = props;
        this.logger = LoggerFactory.getLogger(NLPParser.class);
        this.pipeline = new StanfordCoreNLP(this.props);

        this.logger.info("Instantiated NLPParser!");
      }

      public List<SemanticGraph> parse(String text) {
          CoreDocument document = new CoreDocument(text);
          this.pipeline.annotate(document);

          List<SemanticGraph> graphs = document.sentences().stream()
              .map(s -> s.dependencyParse())
              .collect(Collectors.toList());

          return graphs;
      }
}