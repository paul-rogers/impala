package com.cloudera.cmf.analyzer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudera.cmf.analyzer.ProfileAnalyzer.ExecProfileNode;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.FragmentExecNode;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.InstancesNode;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.OperatorExecNode;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.SummaryNode;
import com.cloudera.cmf.analyzer.QueryPlan.PlanNode;
import com.jolbox.thirdparty.com.google.common.base.Preconditions;

/**
 * Synthesizes query profile information to create a DAG of operators, fragments
 * and servers. Assembles the operator DAG from the parsed plan, the fragment
 * DAG from the operator DAG and the execution details. Allows easy analysis
 * of the query structure.
 */
public class QueryDAG {

  /**
   * Fragment of execution. A fragment contains a subset of the operator
   * DAG assigned to one or more hosts. Each fragment (except the coordinator)
   * has a parent (output) fragment, and each (except leaves) has one or
   * more input fragments.
   * <p>
   * Fragments are assigned to servers.
   * This class tracks the execution details for each of the assigned
   * servers, and the overall operator summary across servers.
   */
  public static class FragmentSynNode {
    private final int fragmentId;
    private final FragmentExecNode summaryNode;
    private InstancesNode detailsNode;
    private final Map<String, FragmentExecNode> details = new HashMap<>();
    private final List<FragmentSynNode> children = new ArrayList<>();
    private final List<OperatorSynNode> operators = new ArrayList<>();
    private OperatorSynNode rootOperator;
    private FragmentSynNode parent;

    public FragmentSynNode(FragmentExecNode summary) {
      fragmentId = summary.fragmentId();
      summaryNode = summary;
    }

    public void defineCoordinator(String serverId) {
      details.put(serverId, summaryNode);
    }

    public void defineDetails(InstancesNode fragDetails) {
      detailsNode = fragDetails;
      for (FragmentExecNode hostDetail : fragDetails.hostNodes()) {
        Preconditions.checkState(! details.containsKey(hostDetail.serverId()));
        details.put(hostDetail.serverId(), hostDetail);
      }
    }

    public Map<String, FragmentExecNode> details() {
      return details;
    }

    public FragmentExecNode summary() { return summaryNode; }
    public int fragmentId() { return fragmentId; }
    public boolean isLeaf() { return children.isEmpty(); }
    public boolean isRoot() { return parent == null; }
    public int hostCount() { return details.size(); }

    public void addOperator(OperatorSynNode opSyn) {
      operators.add(opSyn);
    }

    public void setRootOperator(OperatorSynNode opSyn) {
      this.rootOperator = opSyn;
      if (! opSyn.isRoot()) {
        parent = opSyn.parent().fragment();
        parent.children.add(this);
      }
    }

    public void format(AttribFormatter fmt) {
      fmt.startGroup("Fragment " + fragmentId);
      fmt.list("Servers", details.keySet());
      fmt.startGroup("Operators");
      for (int i = 0; i < operators.size(); i++) {
        OperatorSynNode opSyn = operators.get(i);
        fmt.line(opSyn.planNode().heading());
      }
      fmt.endGroup();
      fmt.endGroup();
    }

    public void formatDag(AttribFormatter fmt) {
      fmt.line(label());
      if (isLeaf()) { return; }
      fmt.startGroup();
      for (FragmentSynNode child : children) {
        child.formatDag(fmt);
      }
      fmt.endGroup();
    }

    public String label() {
      return String.format("%d: (%s) %d hosts",
          fragmentId, OperatorSynNode.idList(operators),
          hostCount());
    }
  }

  /**
   * A single operator. All operators except the root have a parent, and
   * all operators except leaves (scans) have children. An operator is
   * assigned to (one) fragment. A sub-DAG of operators are assigned to
   * together to a fragment, separated by exchanges.
   * <p>
   * Operators are assigned to fragments which are assigned to servers.
   * This class tracks the execution details for each of the assigned
   * servers, and the overall operator summary across servers.
   */
  public static class OperatorSynNode {
    private final int operatorId;
    private FragmentSynNode fragment;
    private final PlanNode planNode;
    private OperatorExecNode summaryNode;
    private final Map<String, OperatorExecNode> details = new HashMap<>();
    private OperatorSynNode children[];
    private OperatorSynNode parent;

    public OperatorSynNode(PlanNode operator) {
      this.operatorId = operator.operatorId();
      planNode = operator;
    }

   public void setSummary(FragmentSynNode fragSyn, OperatorExecNode operExec) {
      fragment = fragSyn;
      Preconditions.checkState(summaryNode == null);
      summaryNode = operExec;
    }

    public void addDetail(String serverId, OperatorExecNode operExec) {
      Preconditions.checkState(! details.containsKey(serverId));
      details.put(serverId, operExec);
    }

    public void setChildren(OperatorSynNode children[]) {
      this.children = children;
      for (OperatorSynNode child : children) {
        child.parent = this;
      }
    }

    public int operatorId() { return operatorId; }
    public PlanNode planNode() { return planNode; }
    public boolean isLeaf() { return children == null; }
    public boolean isRoot() { return parent == null; }
    public OperatorSynNode[] children() { return children; }
    public int fragmentId() { return fragment.fragmentId(); }
    public FragmentSynNode fragment() { return fragment; }
    public OperatorSynNode parent() { return parent; }

    public int childCount() {
      return children == null ? 0 : children.length;
    }

    public boolean isFragmentRoot() {
      return isRoot() || parent.fragmentId() != fragmentId();
    }

    public static String idList(Collection<OperatorSynNode> ops) {
      List<String> opIds = new ArrayList<>();
      for (OperatorSynNode opSyn : ops) {
        opIds.add(Integer.toString(opSyn.operatorId()));
      }
      return String.join(", ", opIds);
    }

    public static String idList(OperatorSynNode ops[]) {
      if (ops == null ) { return "None"; }
      List<String> opIds = new ArrayList<>();
      for (OperatorSynNode opSyn : ops) {
        opIds.add(Integer.toString(opSyn.operatorId()));
      }
      return String.join(", ", opIds);
    }

    public void format(AttribFormatter fmt) {
      fmt.startGroup("Operator " + operatorId);
      fmt.attrib("Fragment", fragment.fragmentId());
      fmt.attrib("Type", planNode.heading());
      fmt.attrib("Parent", isRoot() ? "None" : Integer.toString(parent.operatorId()));
      fmt.attrib("Children", idList(children));
      if (fmt.verbose()) {
        fmt.list("Servers", details.keySet());
      }
      fmt.endGroup();
    }

    public String label() {
      return String.format("%d (%d - %d hosts): %s",
          operatorId, fragmentId(),
          fragment.hostCount(),
          planNode.title());
    }

    public void formatDag(AttribFormatter fmt) {
      fmt.line(label());
      if (isLeaf()) { return; }
      fmt.startGroup();
      for (OperatorSynNode child : children) {
        child.formatDag(fmt);
      }
      fmt.endGroup();
    }

    public void formatFragmentDag(AttribFormatter fmt) {
      fmt.line(planNode.heading());
      if (isLeaf()) { return; }
      boolean first = true;
      for (OperatorSynNode child : children) {
        if (child.isFragmentRoot()) { continue; }
        if (first) {
          fmt.startGroup();
          first = false;
        }
        child.formatDag(fmt);
      }
      if (! first) { fmt.endGroup(); }
    }
  }

  /**
   * Represents a server in the Impala cluster. Each server is
   * assigned one or more fragments to execute. One server is the
   * coordinator.
   */
  public static class ServerSynNode {
    private final String serverId;
    private boolean isCoordinator;
    private List<FragmentSynNode> fragments = new ArrayList<>();

    public ServerSynNode(String serverId) {
      this.serverId = serverId;
    }

    public void addFrag(FragmentSynNode frag) {
      fragments.add(frag);
    }

    public void format(AttribFormatter fmt) {
      fmt.startGroup("Server " + label());
      int frags[] = new int[fragments.size()];
      for (int i = 0; i < fragments.size(); i++) {
        frags[i] = fragments.get(i).fragmentId();
      }
      fmt.attrib("Fragments", frags);
      fmt.endGroup();
    }

    public void defineCoordinator() {
      isCoordinator = true;
    }

    public boolean isCoordinator() { return isCoordinator; }

    public String label() {
      if (isCoordinator) {
        return serverId + " (Coordinator)";
      } else {
        return serverId;
      }
    }
  }

  private final ProfileAnalyzer profile;
  private FragmentSynNode[] fragments;
  private final OperatorSynNode[] operators;
  private FragmentSynNode rootFragment;
  private OperatorSynNode rootOperator;
  private final Map<String, ServerSynNode> servers = new HashMap<>();
  private List<String> serverNames = new ArrayList<>();

  public QueryDAG(ProfileAnalyzer profile) {
    this.profile = profile;
    QueryPlan plan = profile.plan();

    operators = new OperatorSynNode[plan.operatorCount()];
    for (int i = 0; i < operators.length; i++) {
      operators[i] = new OperatorSynNode(plan.operator(i));
    }
    rootOperator = operators[plan.root().operatorId()];

    analyzeDag();
  }

  public void analyzeDag() {
    ExecProfileNode exec = profile.exec();
    setCoordinator(exec.coordinator());
    defineFrags(exec.summaries());
    defineFragDetails(exec.details());
    defineServers();
    gatherOperatorSummaries();
    gatherOperatorDetails();
    stitchOperator(rootOperator);
    stitchFragmentOperator(rootOperator);
  }

  public void setCoordinator(FragmentExecNode coordinator) {
    // Coordinator is the highest-numbered fragment
    fragments = new FragmentSynNode[coordinator.fragmentId() + 1];
    rootFragment = defineFrag(coordinator);
    rootFragment.defineCoordinator(profile.summary().attrib(SummaryNode.Attrib.COORDINATOR));
  }

  private FragmentSynNode defineFrag(FragmentExecNode summary) {
    FragmentSynNode fragSyn = new FragmentSynNode(summary);
    Preconditions.checkState(fragments[summary.fragmentId()] == null);
    fragments[summary.fragmentId()] = fragSyn;
    return fragSyn;
  }

  public void defineFrags(List<FragmentExecNode> summaries) {
    for (FragmentExecNode frag : summaries) {
      defineFrag(frag);
    }
  }

  public void defineFragDetails(List<InstancesNode> details) {
    for (InstancesNode fragDetails : details) {
      fragments[fragDetails.fragmentId()].defineDetails(fragDetails);
    }
  }

  private void defineServers() {
    for (int i = 0; i < fragments.length; i++) {
      defineServers(fragments[i]);
    }
    serverNames.addAll(servers.keySet());
    Collections.sort(serverNames);
    servers.get(profile.summary().attrib(SummaryNode.Attrib.COORDINATOR)).defineCoordinator();
  }

  public void defineServers(FragmentSynNode fragSyn) {
    for (Entry<String, FragmentExecNode> entry : fragSyn.details().entrySet()) {
      ServerSynNode server = servers.get(entry.getKey());
      if (server == null) {
        server = new ServerSynNode(entry.getKey());
        servers.put(entry.getKey(), server);
      }
      server.addFrag(fragSyn);
    }
  }

  private void gatherOperatorSummaries() {
    for (int i = 0; i < fragments.length; i++) {
      FragmentSynNode fragSyn = fragments[i];
      for (OperatorExecNode operExec : fragSyn.summary().operators()) {
        gatherOperatorSummary(fragSyn, operExec);
      }
    }
  }

  private void gatherOperatorSummary(FragmentSynNode fragSyn, OperatorExecNode operExec) {
    OperatorSynNode opSyn = operators[operExec.operatorId()];
    opSyn.setSummary(fragSyn, operExec);
    fragSyn.addOperator(opSyn);
    for (OperatorExecNode child : operExec.children()) {
      gatherOperatorSummary(fragSyn, child);
    }
  }

  private void gatherOperatorDetails() {
    for (int i = 0; i < fragments.length; i++) {
      FragmentSynNode fragSyn = fragments[i];
      for (Entry<String, FragmentExecNode> entry : fragSyn.details().entrySet()) {
        gatherOperatorDetails(entry.getKey(), entry.getValue());
      }
    }
  }

  private void gatherOperatorDetails(String serverId, FragmentExecNode fragExec) {
    for (OperatorExecNode operExec : fragExec.operators()) {
      gatherOperatorDetails(serverId, operExec);
    }
  }

  private void gatherOperatorDetails(String serverId, OperatorExecNode operExec) {
    OperatorSynNode opSyn = operators[operExec.operatorId()];
    opSyn.addDetail(serverId, operExec);
    for (OperatorExecNode child : operExec.children()) {
      gatherOperatorDetails(serverId, child);
    }
  }

  private void stitchOperator(OperatorSynNode opSyn) {
    PlanNode opPlan = opSyn.planNode();
    if (opPlan.isLeaf()) { return; }
    List<PlanNode> planChildren = opPlan.children();
    int childCount = planChildren.size();
    OperatorSynNode children[] = new OperatorSynNode[childCount];
    for (int i = 0; i < childCount; i++) {
      children[i] = operators[planChildren.get(i).operatorId()];
      stitchOperator(children[i]);
    }
    opSyn.setChildren(children);
  }

  private void stitchFragmentOperator(OperatorSynNode opSyn) {
    if (opSyn.isFragmentRoot()) {
      opSyn.fragment().setRootOperator(opSyn);
    }
    if (opSyn.isLeaf()) { return; }
    for (OperatorSynNode child : opSyn.children()) {
      stitchFragmentOperator(child);
    }
  }


  public void print() {
    AttribPrintFormatter fmt = new AttribPrintFormatter();
    format(fmt);
  }

  private void format(AttribFormatter fmt) {
    fmt.startGroup("DAG Synthesys");
    fmt.attrib("Coordinator", rootFragment.fragmentId());
    fmt.attrib("Root Operator", rootOperator.operatorId());
    fmt.attrib("Cluster Size", servers.size());
    fmt.attrib("Fragment Count", fragments.length);
    fmt.attrib("Operator Count", operators.length);

    fmt.startGroup("Fragments");
    for (FragmentSynNode fragSyn : fragments) {
      fragSyn.format(fmt);
    }
    fmt.endGroup();

    fmt.startGroup("Operators");
    for (OperatorSynNode opSyn : operators) {
      opSyn.format(fmt);
    }
    fmt.endGroup();

    fmt.startGroup("Servers");
    for (String serverId : serverNames) {
      servers.get(serverId).format(fmt);
    }
    fmt.endGroup();

    fmt.startGroup("Fragment DAG");
    fmt.startGroup();
    rootFragment.formatDag(fmt);
    fmt.endGroup();
    fmt.endGroup();

    fmt.startGroup("Operator DAG");
    fmt.startGroup();
    rootOperator.formatDag(fmt);
    fmt.endGroup();
    fmt.endGroup();

    fmt.endGroup();
  }
}
