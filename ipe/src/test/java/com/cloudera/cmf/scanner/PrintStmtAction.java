package com.cloudera.cmf.scanner;

import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.scanner.ProfileScanner.AbstractAction;

public class PrintStmtAction extends AbstractAction {

  @Override
  public void apply(ProfileFacade profile) {
    fmt.attrib("Statement", profile.stmt());
  }
}
