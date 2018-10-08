package com.cloudera.cmf.analyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TTimeSeriesCounter;
import org.apache.impala.thrift.TUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Parent class for the three kinds of fragment nodes:
 * <pre>
 * Coordinator Fragment F10
 * Averaged Fragment F09
 * Fragment F08:
 *   Instance ...
 * <pre>
 *
 * Note that the third kind is not really a fragment, rather
 * it is a container of per-host fragment details.
 */
public class FragmentPNode extends ProfileNode {
  /**
   * Details for a fragment. Holds a list of fragment details,
   * one per host.
   *
   * <pre>
   * FRAGMENT F08:
   *   Instance xxx:xxx (host=xxx)
   *   ...
   * </pre>
   */
  public static class InstancesPNode extends FragmentPNode {

    public static final String NAME_PREFIX = "Fragment ";

    // Generated using EnumBuilder
    public enum Counter {
      AVERAGE_THREAD_TOKENS("AverageThreadTokens", TUnit.DOUBLE_VALUE),
      BLOOM_FILTER_BYTES("BloomFilterBytes", TUnit.BYTES),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PER_HOST_PEAK_MEM_USAGE("PerHostPeakMemUsage", TUnit.BYTES),
      PREPARE_TIME("PrepareTime", TUnit.TIME_NS),
      ROWS_PRODUCED("RowsProduced", TUnit.UNIT),
      TOTAL_CPU_TIME("TotalCpuTime", TUnit.TIME_NS),
      TOTAL_NETWORK_RECEIVE_TIME("TotalNetworkReceiveTime", TUnit.TIME_NS),
      TOTAL_NETWORK_SEND_TIME("TotalNetworkSendTime", TUnit.TIME_NS),
      TOTAL_STORAGE_WAIT_TIME("TotalStorageWaitTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    // Generated using EnumBuilder
    public enum TimeSeries {
      MEMORY_USAGE("MemoryUsage", TUnit.BYTES),
      THREAD_USAGE("ThreadUsage", TUnit.UNIT);

      private final String key;
      private TUnit units;

      private TimeSeries(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    protected final List<FragmentPNode.FragInstancePNode> fragments = new ArrayList<>();

    public InstancesPNode(ProfileFacade analyzer, ProfileNode.NodeIndex index) {
      super(analyzer, index.index);
      index.index++;
      Pattern p = Pattern.compile("Fragment F(\\d+)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentId = Integer.parseInt(m.group(1));
      for (int i = 0; i < childCount(); i++) {
        FragmentPNode.FragInstancePNode frag = new FragmentPNode.FragInstancePNode(analyzer, index, fragmentId);
        fragments.add(frag);
      }
    }

    public List<FragmentPNode.FragInstancePNode> hostNodes() {
      return fragments;
    }

    @Override
    public String genericName() { return "Fragment"; }

    @Override
    public List<ProfileNode> childNodes() {
      return Lists.newArrayList(fragments);
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }

    public TTimeSeriesCounter timeSeries(TimeSeries counter) {
      return timeSeries(counter.key());
    }
  }

  /**
   * Execution detail for a single fragment or an average over
   * a set of fragments.
   *
   * <pre>
   * Coordinator Fragment | Averaged Fragment | Instance
   *   BlockMgr
   *   CodeGen
   *   DataStreamSender
   *   &lt;OPERATOR>_NODE
   *   Filter
   * </pre>
   */
  public static abstract class FragDetailsPNode extends FragmentPNode {

    public enum FragmentType {
      COORDINATOR,
      AVERAGED,
      INSTANCE
    }

    protected OperatorPNode.BlockMgrPNode blockMgr;
    protected final List<OperatorPNode> operators = new ArrayList<>();
    private OperatorPNode.CodeGenPNode codeGen;
    private OperatorPNode.DataStreamSenderPNode dataStreamSender;

    public FragDetailsPNode(ProfileFacade analyzer, ProfileNode.NodeIndex index) {
      super(analyzer, index.index++);
      parseChildren(index);
    }

    private void parseChildren(ProfileNode.NodeIndex index) {
      Preconditions.checkState(index.index == firstChild);
      for (int i = 0; i < childCount(); i++) {
        TRuntimeProfileNode profileNode = analyzer.node(index.index);
        String name = profileNode.getName();
        parseChild(name, index);
      }
    }

    protected void parseChild(String name, ProfileNode.NodeIndex index) {
      if (name.equals(OperatorPNode.BlockMgrPNode.NAME)) {
        blockMgr = new OperatorPNode.BlockMgrPNode(analyzer, index.index++);
      } else if (name.equals(OperatorPNode.CodeGenPNode.NAME)) {
        codeGen = new OperatorPNode.CodeGenPNode(analyzer, index.index++);
      } else if (name.startsWith(OperatorPNode.DataStreamSenderPNode.NAME_PREFIX)) {
        dataStreamSender = new OperatorPNode.DataStreamSenderPNode(analyzer, index.index++);
      } else  {
        operators.add(OperatorPNode.parseOperator(analyzer, name, index));
      }
    }

    public List<OperatorPNode> operators() { return operators; }

    public abstract FragmentType fragmentType();

    @Override
    public String genericName() { return fragmentType().name(); }

    @Override
    public List<ProfileNode> childNodes() {
      List<ProfileNode> children = new ArrayList<>();
      if (blockMgr != null) { children.add(blockMgr); }
      if (codeGen != null) { children.add(codeGen); }
      if (dataStreamSender != null) { children.add(dataStreamSender); }
      children.addAll(operators);
      return children;
    }
  }

  /**
   * Execution information for the coordinator of which there is only one.
   * As a result, the coordinator acts as both a summary and detail node.
   * Oddly, the coordinator node does not identify the server on which it
   * runs since it is not under a Fragment node. Instead, obtain the
   * coordinator host from the summary node.
   */
  public static class CoordinatorPNode extends FragDetailsPNode {

    public static final String PREFIX = "Coordinator ";

    // Generated using EnumBuilder
    public enum Counter {
      AVERAGE_THREAD_TOKENS("AverageThreadTokens", TUnit.DOUBLE_VALUE),
      BLOOM_FILTER_BYTES("BloomFilterBytes", TUnit.BYTES),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PER_HOST_PEAK_MEM_USAGE("PerHostPeakMemUsage", TUnit.BYTES),
      PREPARE_TIME("PrepareTime", TUnit.TIME_NS),
      ROWS_PRODUCED("RowsProduced", TUnit.UNIT),
      TOTAL_CPU_TIME("TotalCpuTime", TUnit.TIME_NS),
      TOTAL_NETWORK_RECEIVE_TIME("TotalNetworkReceiveTime", TUnit.TIME_NS),
      TOTAL_NETWORK_SEND_TIME("TotalNetworkSendTime", TUnit.TIME_NS),
      TOTAL_STORAGE_WAIT_TIME("TotalStorageWaitTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    // Generated using EnumBuilder
    public enum TimeSeries {
      MEMORY_USAGE("MemoryUsage", TUnit.BYTES),
      THREAD_USAGE("ThreadUsage", TUnit.UNIT);

      private final String key;
      private TUnit units;

      private TimeSeries(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public CoordinatorPNode(ProfileFacade analyzer, ProfileNode.NodeIndex index) {
      super(analyzer, index);
      Pattern p = Pattern.compile(PREFIX + "Fragment F(\\d+)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentId = Integer.parseInt(m.group(1));
     }

    @Override
    public FragmentType fragmentType() { return FragmentType.COORDINATOR; }

    public long counter(Counter counter) {
      return counter(counter.key());
    }

    public TTimeSeriesCounter timeSeries(TimeSeries counter) {
      return timeSeries(counter.key());
    }
  }

  /**
   * Execution summary for a single fragment. Aggregates the metrics for
   * all instances of a given fragment (except for the coordinator.)
   */
  public static class FragSummaryPNode extends FragDetailsPNode {

    public static final String PREFIX = "Averaged ";

    // Generated using EnumBuilder
    public enum Attrib {
      SPLIT_SIZES("split sizes"),
      COMPLETION_TIMES("completion times"),
      EXECUTION_RATES("execution rates"),
      NUM_INSTANCES("num instances");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum Counter {
      AVERAGE_THREAD_TOKENS("AverageThreadTokens", TUnit.DOUBLE_VALUE),
      BLOOM_FILTER_BYTES("BloomFilterBytes", TUnit.BYTES),
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      PEAK_MEMORY_USAGE("PeakMemoryUsage", TUnit.BYTES),
      PER_HOST_PEAK_MEM_USAGE("PerHostPeakMemUsage", TUnit.BYTES),
      PREPARE_TIME("PrepareTime", TUnit.TIME_NS),
      ROWS_PRODUCED("RowsProduced", TUnit.UNIT),
      TOTAL_CPU_TIME("TotalCpuTime", TUnit.TIME_NS),
      TOTAL_NETWORK_RECEIVE_TIME("TotalNetworkReceiveTime", TUnit.TIME_NS),
      TOTAL_NETWORK_SEND_TIME("TotalNetworkSendTime", TUnit.TIME_NS),
      TOTAL_STORAGE_WAIT_TIME("TotalStorageWaitTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    public FragSummaryPNode(ProfileFacade analyzer, ProfileNode.NodeIndex index) {
      super(analyzer, index);
      Pattern p = Pattern.compile(PREFIX + "Fragment F(\\d+)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentId = Integer.parseInt(m.group(1));
    }

    @Override
    public FragmentType fragmentType() { return FragmentType.AVERAGED; }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  /**
   * Execution details for one fragment instance. One of these nodes exist
   * for each (fragment, host) pair.
   */
  public static class FragInstancePNode extends FragDetailsPNode {

    // Generated using EnumBuilder
    public enum Attrib {
      HDFS_SPLIT_STATS("Hdfs split stats (<volume id>:<# splits>/<split lengths>)");

      private final String key;

      private Attrib(String key) {
        this.key = key;
      }

      public String key() { return key; }
    }

    // Generated using EnumBuilder
    public enum Counter {
      INACTIVE_TOTAL_TIME("InactiveTotalTime", TUnit.TIME_NS),
      TOTAL_TIME("TotalTime", TUnit.TIME_NS);

      private final String key;
      private TUnit units;

      private Counter(String key, TUnit units) {
        this.key = key;
        this.units = units;
      }

      public String key() { return key; }
      public TUnit units() { return units; }
    }

    protected String fragmentGuid;
    protected String serverId;

    public FragInstancePNode(ProfileFacade analyzer, ProfileNode.NodeIndex index, int fragmentId) {
      super(analyzer, index);
      this.fragmentId = fragmentId;
      Pattern p = Pattern.compile("Instance (\\S+) \\(host=([^(]+)\\)");
      Matcher m = p.matcher(name());
      Preconditions.checkState(m.matches());
      fragmentGuid = m.group(1);
      serverId = m.group(2);
    }

    @Override
    public FragmentType fragmentType() {
      return FragmentType.INSTANCE;
    }

    public String serverId() { return serverId; }

    public String attrib(Attrib attrib) {
      return attrib(attrib.key());
    }

    public String filterArrival(int filterNo) {
      return attrib("Filter " + filterNo + " arrival");
    }

    public long counter(Counter counter) {
      return counter(counter.key());
    }
  }

  protected int fragmentId;

  public FragmentPNode(ProfileFacade analyzer, int index) {
    super(analyzer, index);
  }

  public int fragmentId() { return fragmentId; }
}
