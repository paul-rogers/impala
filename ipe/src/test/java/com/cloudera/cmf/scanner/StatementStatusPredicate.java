package com.cloudera.cmf.scanner;

import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.profile.ProfileFacade.SummaryState;
import com.cloudera.cmf.scanner.ProfileScanner.Predicate;

public class StatementStatusPredicate implements Predicate {

  private final SummaryState state;

  public StatementStatusPredicate(SummaryState state) {
    this.state = state;
  }

  @Override
  public boolean accept(ProfileFacade profile) {
    return profile.summary().summaryState() == state;
  }

  public static Predicate completedOnly() {
    return new StatementStatusPredicate(SummaryState.OK);
  }

  public static Predicate failedOnly() {
    return new StatementStatusPredicate(SummaryState.FAILED);
  }
}