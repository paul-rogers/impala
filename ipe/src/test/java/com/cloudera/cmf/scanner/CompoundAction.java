package com.cloudera.cmf.scanner;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.cmf.analyzer.ProfileAnalyzer;
import com.cloudera.cmf.scanner.ProfileScanner.Action;

public class CompoundAction implements Action {

  private final List<Action> actions = new ArrayList<>();

  public CompoundAction add(Action action) {
    actions.add(action);
    return this;
  }

  @Override
  public void apply(ProfileAnalyzer profile) {
    for (Action action : actions) {
      action.apply(profile);
    }
  }
}
