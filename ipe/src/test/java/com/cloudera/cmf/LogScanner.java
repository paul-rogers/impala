package com.cloudera.cmf;

import java.io.File;
import java.io.IOException;

import org.apache.impala.thrift.TRuntimeProfileTree;

import com.cloudera.cmf.LogReader.QueryRecord;

public class LogScanner {

  public static interface Predicate {
    boolean accept(ProfileAnalyzer profile);
  }

  public static interface Action {
    void apply(ProfileAnalyzer profile);
  }

  public static class NullRule implements Predicate, Action {

    @Override
    public boolean accept(ProfileAnalyzer profile) {
      return false;
    }

    @Override
    public void apply(ProfileAnalyzer profile) {
    }
  }

  private final File logFile;
  private Predicate predicate;
  private Action action;
  private int skipCount;
  private int scanLimit = Integer.MAX_VALUE;
  private int scanCount;
  private int acceptCount;

  public LogScanner(File logFile) {
    this.logFile = logFile;
    NullRule nullRule = new NullRule();
    predicate = nullRule;
    action = nullRule;
  }

  public LogScanner predicate(Predicate pred) {
    predicate = pred;
    return this;
  }

  public LogScanner action(Action action) {
    this.action = action;
    return this;
  }

  public LogScanner skip(int skip) {
    skipCount = skip;
    return this;
  }

  public LogScanner limit(int limit) {
    scanLimit = limit;
    return this;
  }

  public void scan() throws IOException {
    scanCount = 0;
    acceptCount = 0;
    LogReader lr = new LogReader(logFile);
    lr.skip(skipCount);
    int posn = skipCount - 1;
    for (;;) {
      if (scanCount >= scanLimit) { break; }
      QueryRecord qr = lr.next();
      if (qr == null) { break; }
      posn++;
      scanCount++;
      TRuntimeProfileTree profile = qr.thriftProfile();
      String source = String.format("%s[%d]",
          logFile.getName(), posn);
      ProfileAnalyzer analyzer = new ProfileAnalyzer(profile,
          qr.queryId(), source);
      boolean accept = predicate.accept(analyzer);
      logResult(analyzer, accept);
      if (accept) {
        acceptCount++;
        action.apply(analyzer);
      }
    }
  }

  private void logResult(ProfileAnalyzer analyzer, boolean accept) {
    System.out.println(String.format("%s - %s",
        analyzer.title(),
        accept ? "Accept" : "Skip"));

  }

  public int limit() { return scanLimit; }
  public int scanCount() { return scanCount; }
  public int acceptCount() { return acceptCount; }
}
