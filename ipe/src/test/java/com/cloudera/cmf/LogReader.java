package com.cloudera.cmf;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.impala.thrift.TRuntimeProfileTree;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.joda.time.Instant;

import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.rules.ImpalaRuntimeProfile;
import com.cloudera.ipe.util.ThriftUtil;
import com.google.common.base.Splitter;

public class LogReader {
  private final static TProtocolFactory PROTOCOL_FACTORY =
      new TCompactProtocol.Factory();

  private static class ShimInputStream extends InputStream {

    private final InputStream in;
    private boolean eof;

    public ShimInputStream(InputStream in) {
      this.in = in;
    }

    @Override
    public int read() throws IOException {
      int b = (eof) ? -1 : in.read();
      if (b == -1 || b == '\n') {
        eof = true;
        return -1;
      }
      return b;
    }

    @Override
    public void close() throws IOException {
      // Do not close input, it belongs to the
      // log reader
    }
  }

  public static class QueryRecord {
    protected String timestamp;
    protected String queryId;
    protected final LogReader logReader;

    public QueryRecord(LogReader logReader,
        String logTime,
        String queryId) {
      this.logReader = logReader;
      this.queryId = queryId;
      this.timestamp = logTime;
    }

    public String queryId() { return queryId; }
    public String timestampStr() { return timestamp; }

    public Instant timestamp() {
      Double ts = Double.parseDouble(timestamp);
      return new Instant(Math.round(ts * 1000));
    }

    public InputStream data() {
      InputStream is = new ShimInputStream(logReader.in);
      is = new Base64InputStream(is);
      is = new InflaterInputStream(is);
      return is;
    }

    public void skipData() throws IOException {
      InputStream is = data();
      while (is.read() != -1) { }
      is.close();
    }

    public TRuntimeProfileTree thriftProfile() throws IOException {
      TRuntimeProfileTree thriftProfile = new TRuntimeProfileTree();
      ThriftUtil.read(data(), thriftProfile, PROTOCOL_FACTORY);
      return thriftProfile;
    }

    public ImpalaRuntimeProfile profile() throws IOException {
      return new ImpalaRuntimeProfile(thriftProfile(),
          logReader.serviceName(), logReader.frontEndHostId(),
          timestamp(), new Instant(),
          ImpalaRuntimeProfile.DEFAULT_TIME_FORMATS,
          ImpalaRuntimeProfileTree.MILLISECOND_TIME_FORMATTER);
    }
  }

  private final String serviceName;
  private final String frontEndHostId;
  private final InputStream in;

  public LogReader(File inFile) throws FileNotFoundException {
    this("Impala", "hot", inFile);
  }

  public LogReader(String serviceName, String frontEndHostId, File inFile)
      throws FileNotFoundException {
    this.serviceName = serviceName;
    this.frontEndHostId = frontEndHostId;
    InputStream is = new FileInputStream(inFile);
    in = new BufferedInputStream(is);
  }

  public String serviceName() { return serviceName; }
  public String frontEndHostId() { return frontEndHostId; }

  public QueryRecord next() throws IOException {
    String timestamp = readTo(' ');
    if (timestamp == null) { return null; }
    return new QueryRecord(this, timestamp,
        readTo(' '));
  }

  private String readTo(char term) throws IOException {
    StringBuilder buf = new StringBuilder();
    for (;;) {
      int b = in.read();
      if (b == -1) {
        return null;
      }
      if (b == term) {
        return buf.toString();
      }
      buf.append((char) b);
    }
  }

  public void close() throws IOException {
    in.close();
  }

  public void skip(int n) throws IOException {
    for (int i = 0; i < n; i++) {
      QueryRecord qr = next();
      if (qr == null) { break; }
      qr.skipData();
    }
  }
}