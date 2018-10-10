package com.cloudera.cmf.scanner;

import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.profile.ProfileFacade.QueryType;
import com.cloudera.cmf.scanner.ProfileScanner.Predicate;

public class StatementTypePredicate implements Predicate {

  private final QueryType type;

  public StatementTypePredicate(QueryType type) {
    this.type = type;
  }

  @Override
  public boolean accept(ProfileFacade profile) {
    return profile.type() == type;
  }

  public static Predicate selectOnly() {
    return new StatementTypePredicate(QueryType.QUERY);
  }
}
