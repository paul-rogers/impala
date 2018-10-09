package com.cloudera.cmf.scanner;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TUnit;

public interface Aggregate {
  void add(TCounter counter);
  void finish();

  public static class Summation implements Aggregate {

    private final String name;
    private final TUnit units;
    protected long total;

    public Summation(String name, TUnit units) {
      this.name = name;
      this.units = units;
    }

    @Override
    public void add(TCounter counter) {
      total += counter.getValue();
    }

    @Override
    public void finish() {
    }

    public long total() { return total; }

  }

  public static class Average extends Summation {

    private int count;

    public Average(String name, TUnit units) {
      super(name, units);
    }

    @Override
    public void add(TCounter counter) {
      super.add(counter);
      count++;
    }

    public double average() {
      if (count == 0) { return 0; }
      return (double) total / count;
    }
  }
}
