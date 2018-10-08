package com.cloudera.cmf.scanner;

import java.io.File;
import java.io.IOException;

import org.apache.impala.thrift.TRuntimeProfileTree;

import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.scanner.LogReader.QueryRecord;
import com.jolbox.thirdparty.com.google.common.base.Preconditions;

public class ProfileScanner {

  public static interface FilePredicate {
    boolean accept(File file);
  }

  public static interface Predicate {
    boolean accept(ProfileFacade profile);
  }

  public static interface Action {
    void apply(ProfileFacade profile);
  }

  public static interface Monitor {
    void log(String msg);
  }

  public static class NullRule implements Predicate, Action, Monitor, FilePredicate {

    @Override
    public boolean accept(ProfileFacade profile) {
      return false;
    }

    @Override
    public void apply(ProfileFacade profile) {
    }

    @Override
    public void log(String msg) {
    }

    @Override
    public boolean accept(File file) {
      return ! file.isDirectory();
    }
  }

  public static class StdOutMonitor implements Monitor {

    @Override
    public void log(String msg) {
      System.out.println(msg);
    }
  }

  public static abstract class BaseScanner {

    public ProfileScanner root;

    public BaseScanner(ProfileScanner root) {
      this.root = root;
    }

    public abstract void scan() throws IOException;
  }

  public static class DirScanner extends BaseScanner {

    private final File dir;

    public DirScanner(ProfileScanner root, File dir) {
      super(root);
      this.dir = dir;
      Preconditions.checkArgument(dir.isDirectory());
    }

    @Override
    public void scan() throws IOException {
      root.tallyDir();
      File files[] = dir.listFiles();
      for (File file : files) {
        if (! root.shouldContinue()) {
          break;
        }
        if (! root.filePredicate().accept(file)) {
          continue;
        }
        if (file.isDirectory()) {
          DirScanner dirScanner = new DirScanner(root, file);
          dirScanner.scan();
        } else {
          FileScanner fileScanner = new FileScanner(root, file);
          fileScanner.scan();
        }
      }
    }
  }

  public static class FileScanner extends BaseScanner {

    private final File logFile;

    public FileScanner(ProfileScanner root, File logFile) {
      super(root);
      this.logFile = logFile;
      Preconditions.checkArgument(logFile.isFile());
    }

    @Override
    public void scan() throws IOException {
      root.tallyFile();
      LogReader lr = new LogReader(logFile);
      lr.skip(root.skipCount());
      int posn = root.skipCount() - 1;
      for (;;) {
        if (! root.shouldContinue()) { break; }
        QueryRecord qr = lr.next();
        if (qr == null) { break; }
        posn++;
        TRuntimeProfileTree profile = qr.thriftProfile();
        String source = String.format("%s[%d]",
            logFile.getName(), posn);
        QueryScanner queryScanner = new QueryScanner(
            root, profile, source, qr.queryId());
        queryScanner.scan();
      }
      lr.close();
    }
  }

  public static class QueryScanner extends BaseScanner {

    private final TRuntimeProfileTree profile;
    private final String label;
    private final String queryId;

    public QueryScanner(ProfileScanner root, TRuntimeProfileTree profile,
        String label, String queryId) {
      super(root);
      this.profile = profile;
      this.label = label;
      this.queryId = queryId;
    }

    @Override
    public void scan() {
      root.tallyStatement();
      ProfileFacade analyzer = new ProfileFacade(profile,
          queryId, label);
      boolean accept = root.predicate().accept(analyzer);
      logResult(analyzer, accept);
      if (accept) {
        root.tallyAccept();
        root.action().apply(analyzer);
      }
    }

    private void logResult(ProfileFacade analyzer, boolean accept) {
      String msg = String.format("%s - %s",
          analyzer.title(),
          accept ? "Accept" : "Skip");
      root.monitor().log(msg);
    }
  }

  private static class NullScanner extends BaseScanner {

    public NullScanner(ProfileScanner root) {
      super(root);
    }

    @Override
    public void scan() throws IOException { }
  }

  private BaseScanner scanner;
  private Predicate predicate;
  private Action action;
  private Monitor monitor;
  private FilePredicate filePredicate;
  private int dirCount;
  private int fileCount;
  private int skipCount;
  private int scanLimit = Integer.MAX_VALUE;
  private int scanCount;
  private int acceptCount;

  public ProfileScanner() {
    NullRule nullRule = new NullRule();
    predicate = nullRule;
    action = nullRule;
    monitor = nullRule;
    filePredicate = nullRule;
    scanner = new NullScanner(this);
  }

  public ProfileScanner scanFile(File file) {
    if (! file.exists()) {
      scanner = new NullScanner(this);
    } else if (file.isDirectory()) {
      scanner= new DirScanner(this, file);
    } else {
      scanner = new FileScanner(this, file);
    }
    return this;
  }

  public ProfileScanner filePredicate(FilePredicate filePredicate) {
    this.filePredicate = filePredicate;
    return this;
  }

  public ProfileScanner predicate(Predicate pred) {
    predicate = pred;
    return this;
  }

  public ProfileScanner action(Action action) {
    this.action = action;
    return this;
  }

  public ProfileScanner monitor(Monitor monitor) {
    this.monitor = monitor;
    return this;
  }

  public ProfileScanner toConsole() {
    return monitor(new StdOutMonitor());
  }

  public ProfileScanner skip(int skip) {
    skipCount = skip;
    return this;
  }

  public ProfileScanner limit(int limit) {
    scanLimit = limit;
    return this;
  }

  public void scan() throws IOException {
    dirCount = 0;
    fileCount = 0;
    scanCount = 0;
    acceptCount = 0;
    scanner.scan();
  }

  public int dirCount() { return dirCount; }
  public int fileCount() { return fileCount; }
  public int scanCount() { return scanCount; }
  public int acceptCount() { return acceptCount; }

  public int limit() { return scanLimit; }
  public int skipCount() { return skipCount; }
  protected void tallyDir() { dirCount++; }
  protected void tallyFile() { fileCount++; }
  protected void tallyStatement() { scanCount++; }
  protected void tallyAccept() { acceptCount++; }
  protected Predicate predicate() { return predicate; }
  protected Action action() { return action; }
  protected Monitor monitor() { return monitor; }
  protected FilePredicate filePredicate() { return filePredicate; }

  protected boolean shouldContinue() {
    return acceptCount < scanLimit;
  }
}
