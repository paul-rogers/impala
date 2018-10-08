package com.cloudera.cmf.analyzer;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TUnit;

public class ExecPNode extends ProfileNode {

  // Generated using EnumBuilder
  public enum Attrib {
    NUMBER_OF_FILTERS("Number of filters"),
    FILTER_ROUTING_TABLE("Filter routing table"),
    FRAGMENT_START_LATENCIES("Fragment start latencies"),
    FINAL_FILTER_TABLE("Final filter table"),
    PER_NODE_PEAK_MEMORY_USAGE("Per Node Peak Memory Usage");

    private final String key;

    private Attrib(String key) {
      this.key = key;
    }

    public String key() { return key; }
  }

  // Generated using EnumBuilder
  public enum Counter {
    FILTERS_RECEIVED("FiltersReceived", TUnit.UNIT),
    FINALIZATION_TIMER("FinalizationTimer", TUnit.TIME_NS),
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

  public boolean expanded;
  private FragmentPNode.CoordinatorPNode coordinator;
  private final List<FragmentPNode.FragSummaryPNode> summaries = new ArrayList<>();
  private final List<FragmentPNode.InstancesPNode> details = new ArrayList<>();

  public ExecPNode(ProfileFacade analyzer, int index) {
    super(analyzer, index);
  }

  public void expand() {
    if (expanded) { return; }
    ProfileNode.NodeIndex index = new ProfileNode.NodeIndex(firstChild);
    for (int i = 0; i < childCount(); i++) {
      TRuntimeProfileNode profileNode = analyzer.node(index.index);
      String name = profileNode.getName();
      if (name.startsWith(FragmentPNode.CoordinatorPNode.PREFIX)) {
        coordinator = new FragmentPNode.CoordinatorPNode(analyzer, index);
      } else  if (name.startsWith(FragmentPNode.InstancesPNode.NAME_PREFIX)) {
        details.add(new FragmentPNode.InstancesPNode(analyzer, index));
      } else if (name.startsWith(FragmentPNode.FragSummaryPNode.PREFIX)) {
        summaries.add(new FragmentPNode.FragSummaryPNode(analyzer, index));
      } else {
        throw new IllegalStateException("Exec node type");
      }
    }
    expanded = true;
  }

  public FragmentPNode.CoordinatorPNode coordinator() { return coordinator; }
  public List<FragmentPNode.FragSummaryPNode> summaries() { return summaries; }
  public List<FragmentPNode.InstancesPNode> details() { return details; }

  @Override
  public String genericName() { return "Execution Profile"; }

  public String attrib(Attrib attrib) {
    return attrib(attrib.key());
  }


  public long counter(Counter counter) {
    return counter(counter.key());
  }

  @Override
  public List<ProfileNode> childNodes() {
    List<ProfileNode> children = new ArrayList<>();
    children.add(coordinator);
    children.addAll(summaries);
    children.addAll(details);
    return children;
  }
}
