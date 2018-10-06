package com.cloudera.cmf.scanner;

import com.cloudera.cmf.analyzer.ProfileAnalyzer;
import com.cloudera.cmf.scanner.ProfileScanner.Action;

public class PrintStmtAction implements Action {

  @Override
  public void apply(ProfileAnalyzer profile) {
    System.out.println(profile.stmt());
  }
}
