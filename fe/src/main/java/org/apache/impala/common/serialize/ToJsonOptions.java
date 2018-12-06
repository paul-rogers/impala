package org.apache.impala.common.serialize;

public class ToJsonOptions {

  private boolean showSource_;
  private boolean showOutput_;
  private boolean showInternals_;
  private boolean elide_;
  private boolean dedup_;

  public ToJsonOptions showSource(boolean flag) { showSource_ = flag; return this; }
  public ToJsonOptions showOutput(boolean flag) { showOutput_ = flag; return this; }
  public ToJsonOptions showInternals(boolean flag) { showInternals_ = flag; return this; }
  public ToJsonOptions elide(boolean flag) { elide_ = flag; return this; }
  public ToJsonOptions dedup(boolean flag) { dedup_ = flag; return this; }

  public static ToJsonOptions full() {
    return new ToJsonOptions()
        .showSource(true)
        .showOutput(true)
        .showInternals(true);
  }

  public static ToJsonOptions fullCompact() {
    return full().elide(true).dedup(true);
  }

  public boolean showSource() { return showSource_; }
  public boolean showOutput() { return showOutput_; }
  public boolean showInternals() { return showInternals_; }
  public boolean elide() { return elide_; }
  public boolean dedup() { return dedup_;}
}
