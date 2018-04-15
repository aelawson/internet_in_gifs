package org.aelawson.util;

import java.util.List;

public class SemanticSummary {
    public String subject;
    public List<String> dependencies;

    public SemanticSummary(String subject, List<String> dependencies) {
        this.subject = subject;
        this.dependencies = dependencies;
    }
}