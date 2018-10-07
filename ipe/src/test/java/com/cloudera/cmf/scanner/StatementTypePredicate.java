package com.cloudera.cmf.scanner;

import com.cloudera.cmf.analyzer.ProfileAnalyzer;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.QueryType;
import com.cloudera.cmf.scanner.ProfileScanner.Predicate;

public class StatementTypePredicate implements Predicate {

  private final QueryType type;

  public StatementTypePredicate(QueryType type) {
    this.type = type;
  }

  @Override
  public boolean accept(ProfileAnalyzer profile) {
    return profile.summary().type() == type;
  }

  public static Predicate selectOnly() {
    return new StatementTypePredicate(QueryType.QUERY);
  }
}
