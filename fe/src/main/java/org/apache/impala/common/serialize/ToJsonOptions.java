package org.apache.impala.common.serialize;

public class ToJsonOptions {

  public boolean showSource() {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean showOutput() {
    // TODO Auto-generated method stub
    return true;
  }

  public static ToJsonOptions full() {
    // TODO Auto-generated method stub
    return new ToJsonOptions();
  }

  public boolean showInternals() {
    return true;
  }

  public boolean elide() {
    // TODO Auto-generated method stub
    return true;
  }

}
