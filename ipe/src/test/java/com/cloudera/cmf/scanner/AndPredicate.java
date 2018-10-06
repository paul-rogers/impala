package com.cloudera.cmf.scanner;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.cmf.analyzer.ProfileAnalyzer;
import com.cloudera.cmf.scanner.ProfileScanner.Predicate;

public class AndPredicate implements Predicate {

  List<Predicate> predicates = new ArrayList<>();

  public AndPredicate add(Predicate pred) {
    predicates.add(pred);
    return this;
  }

  @Override
  public boolean accept(ProfileAnalyzer profile) {
    for (Predicate pred : predicates) {
      if (! pred.accept(profile)) { return false; }
    }
    return true;
  }
}
