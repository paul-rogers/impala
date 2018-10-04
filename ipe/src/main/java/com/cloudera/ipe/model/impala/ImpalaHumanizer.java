package com.cloudera.ipe.model.impala;

public interface ImpalaHumanizer {
  public abstract  String humanizeCounter(ImpalaRuntimeProfileCounter counter);

  public abstract String humanizeNanoseconds(long value);

  public abstract String humanizeNameWithTime(Long time, String name);
}
