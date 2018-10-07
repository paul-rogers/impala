package com.cloudera.cmf.analyzer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cloudera.cmf.analyzer.ProfileAnalyzer.ExecProfileNode;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.FragmentExecNode;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.InstancesNode;
import com.cloudera.cmf.analyzer.ProfileAnalyzer.OperatorExecNode;
import com.cloudera.cmf.analyzer.QueryPlan.PlanNode;
import com.jolbox.thirdparty.com.google.common.base.Preconditions;

public class QueryDAG {

  public static class FragmentSynNode {
    private final int fragmentId;
    private final FragmentExecNode summaryNode;
    private InstancesNode detailsNode;
    private final Map<String, FragmentExecNode> details = new HashMap<>();
    private final List<FragmentSynNode> inputs = new ArrayList<>();
    private final List<OperatorSynNode> operators = new ArrayList<>();

    public FragmentSynNode(FragmentExecNode summary) {
      fragmentId = summary.fragmentId();
      summaryNode = summary;
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

    public void format(AttribFormatter fmt) {
      fmt.startGroup("Fragment " + fragmentId);
      fmt.list("Servers", details.keySet());
      int ops[] = new int[operators.size()];
      for (int i = 0; i < operators.size(); i++) {
        ops[i] = operators.get(i).operatorId();
      }
      fmt.attrib("Operators", ops);
      fmt.endGroup();
    }

    public void addOperator(OperatorSynNode opSyn) {
      operators.add(opSyn);
    }
  }

  public static class OperatorSynNode {
    private final int operatorId;
    private FragmentSynNode fragment;
    private final PlanNode planNode;
    private OperatorExecNode summaryNode;
    private final Map<String, OperatorExecNode> details = new HashMap<>();

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

    public int operatorId() { return operatorId; }

    public void format(AttribFormatter fmt) {
      fmt.startGroup("Operator " + operatorId);
      fmt.attrib("Fragment", fragment.fragmentId());
      fmt.attrib("Type", planNode.heading());
      fmt.list("Servers", details.keySet());
      fmt.endGroup();
    }
  }

  public static class ServerSynNode {
    private final String serverId;
    private List<FragmentSynNode> fragments = new ArrayList<>();

    public ServerSynNode(String serverId) {
      this.serverId = serverId;
    }

    public void addFrag(FragmentSynNode frag) {
      fragments.add(frag);
    }

    public void format(AttribFormatter fmt) {
      fmt.startGroup("Server " + serverId);
      int frags[] = new int[fragments.size()];
      for (int i = 0; i < fragments.size(); i++) {
        frags[i] = fragments.get(i).fragmentId();
      }
      fmt.attrib("Fragments", frags);
      fmt.endGroup();
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
  }

  public void setCoordinator(FragmentExecNode coordinator) {
    // Coordinator is the highest-numbered fragment
    fragments = new FragmentSynNode[coordinator.fragmentId() + 1];
    rootFragment = defineFrag(coordinator);
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

  public void print() {
    AttribPrintFormatter fmt = new AttribPrintFormatter();
    format(fmt);
  }

  private void format(AttribFormatter fmt) {
    fmt.attrib("Coordinator", rootFragment.fragmentId());
    fmt.attrib("Root Operator", rootOperator.operatorId());
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
  }
}
