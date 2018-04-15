package org.aelawson.util;

import java.util.List;

public class SemanticSummary {
    public String subject;

    public SemanticSummary(String subject) {
        this.subject = subject;
    }

    @Override
    public String toString() {
        return this.subject;
    }
}