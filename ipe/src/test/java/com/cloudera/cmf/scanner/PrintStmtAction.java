package com.cloudera.cmf.scanner;

import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.scanner.ProfileScanner.Action;

public class PrintStmtAction implements Action {

  @Override
  public void apply(ProfileFacade profile) {
    System.out.println(profile.stmt());
  }
}
