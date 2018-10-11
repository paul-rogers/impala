package com.cloudera.cmf.profile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.impala.thrift.TRuntimeProfileNode;

import com.cloudera.cmf.printer.AttribFormatter;
import com.cloudera.cmf.printer.AttribPrintFormatter;
import com.cloudera.cmf.profile.AbstractFragPNode.FragmentPNode;
import com.cloudera.cmf.profile.OperatorPNode.OperType;
import com.cloudera.cmf.profile.AbstractFragPNode.FragInstancesPNode;
import com.cloudera.cmf.profile.QueryPlan.PlanNode;
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
  public abstract static class FragmentSynNode {

    protected final int fragmentId;
    private final List<FragmentSynNode> children = new ArrayList<>();
    private final List<OperatorSynNode> operators = new ArrayList<>();
    private OperatorSynNode rootOperator;

    public FragmentSynNode(int fragmentId) {
      this.fragmentId = fragmentId;
    }

    public abstract AbstractFragPNode.FragmentPNode averages();
    public int fragmentId() { return fragmentId; }
    public boolean isLeaf() { return children.isEmpty(); }
    public abstract FragmentSynNode parent();
    public abstract boolean isRoot();
    public abstract int hostCount();

    public void addOperator(OperatorSynNode opSyn) {
      operators.add(opSyn);
    }

    public void setRootOperator(OperatorSynNode opSyn) {
      this.rootOperator = opSyn;
    }

    public abstract void format(AttribFormatter fmt);

    protected void formatDetails(AttribFormatter fmt) {
      fmt.attrib("Root Operator", rootOperator.operatorId());
      fmt.startGroup("Operators");
      for (int i = 0; i < operators.size(); i++) {
        OperatorSynNode opSyn = operators.get(i);
        fmt.line(opSyn.planNode().heading());
      }
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

  public static class RootFragSynNode extends FragmentSynNode {

    private final FragmentPNode coord;
    private String serverId;

    public RootFragSynNode(FragmentPNode coord, String serverId) {
      super(coord.fragmentId());
      this.coord = coord;
      this.serverId = serverId;
    }

    public FragmentPNode coord() { return coord; }
    @Override
    public FragmentPNode averages() { return coord; }
    @Override
    public FragmentSynNode parent() { return null; }
    @Override
    public int hostCount() { return 1; }
    public String serverId() { return serverId; }
    @Override
    public boolean isRoot() { return true; }

    @Override
    public void format(AttribFormatter fmt) {
      fmt.startGroup("Coordinator " + fragmentId);
      formatDetails(fmt);
      fmt.attrib("Server", serverId);
      fmt.endGroup();
    }
  }

  public static class InternalFragSynNode extends FragmentSynNode {

    private final FragmentPNode avgNode;
    private FragInstancesPNode detailsNode;
    private final Map<String, FragmentPNode> details = new HashMap<>();
    private FragmentSynNode parent;

    public InternalFragSynNode(FragmentPNode summary) {
      super(summary.fragmentId());
      avgNode = summary;
    }

    public void defineDetails(AbstractFragPNode.FragInstancesPNode fragDetails) {
      detailsNode = fragDetails;
      for (FragmentPNode hostDetail : fragDetails.hostNodes()) {
        Preconditions.checkState(! details.containsKey(hostDetail.serverId()));
        details.put(hostDetail.serverId(), hostDetail);
      }
    }

    public Map<String, FragmentPNode> details() {
      return details;
    }

    @Override
    public FragmentPNode averages() { return avgNode; }
    @Override
    public FragmentSynNode parent() { return parent; }
    @Override
    public int hostCount() { return details.size(); }
    @Override
    public boolean isRoot() { return false; }

    @Override
    public void setRootOperator(OperatorSynNode opSyn) {
      super.setRootOperator(opSyn);
      if (! opSyn.isRoot()) {
        parent = opSyn.parent().fragment();
        parent.children.add(this);
      }
    }

    @Override
    public void format(AttribFormatter fmt) {
      fmt.startGroup("Fragment " + fragmentId);
      formatDetails(fmt);
      fmt.list("Servers", details.keySet());
      fmt.endGroup();
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
    private OperatorPNode avgNode;
    private final Map<String, OperatorPNode> details = new HashMap<>();
    private OperatorSynNode children[];
    private OperatorSynNode parent;
    private OperatorPNode totals;

    public OperatorSynNode(PlanNode operator) {
      this.operatorId = operator.operatorId();
      planNode = operator;
    }

   public void setSummary(FragmentSynNode fragSyn, OperatorPNode operExec) {
      fragment = fragSyn;
      Preconditions.checkState(avgNode == null);
      avgNode = operExec;
    }

    public void addDetail(String serverId, OperatorPNode operExec) {
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
    public String title() { return planNode.title(); }
    public PlanNode planNode() { return planNode; }
    public boolean isLeaf() { return children == null; }
    public boolean isRoot() { return parent == null; }
    public OperatorSynNode[] children() { return children; }
    public int fragmentId() { return fragment.fragmentId(); }
    public FragmentSynNode fragment() { return fragment; }
    public OperatorSynNode parent() { return parent; }
    public OperatorPNode avgNode() { return avgNode; }
    public Collection<OperatorPNode> details() { return details.values(); }
    public int instanceCount() { return details.size(); }
    public OperType opType() { return avgNode.operType(); }

    public int childCount() {
      return children == null ? 0 : children.length;
    }

    public boolean isFragmentRoot() {
      return isRoot() || parent.fragmentId() != fragmentId();
    }

    public OperatorPNode totals() {
      if (totals != null) { return totals; }
      totals = new OperatorPNode(avgNode);
      NodeAggregator agg = new NodeAggregator(totals.node());
      for (OperatorPNode node : details.values()) {
        agg.add(node.node());
      }
      return totals;
    }

    public long avgCounter(String name) {
      return avgNode.counter(name);
    }

    public long totalsCounter(String name) {
      return totals().counter(name);
    }

    public long total(Attrib.Counter counter) {
      return totals().counter(counter);
    }

    public long estTotal(Attrib.Counter counter) {
      return avgNode.counter(counter) * instanceCount();
    }

    public OperatorSynNode child() {
      Preconditions.checkState(childCount() == 1);
      return children[0];
    }

    /**
     * Left child, which is the build side of a hash join.
     */
    public OperatorSynNode leftChild() {
      Preconditions.checkState(childCount() == 2);
      return children[0];
    }

    /**
     * Right child, which is the probe side of a hash join.
     */
    public OperatorSynNode rightChild() {
      Preconditions.checkState(childCount() == 2);
      return children[1];
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

    public String fileFormat() {
      if (opType() != OperType.HDFS_SCAN) { return "N/A"; }
      String format = details().iterator().next().attrib(Attrib.HdfsScanAttrib.FILE_FORMATS);
      int posn = format.indexOf("/");
      if (posn == -1) { return "Unknown"; }
      return format.substring(0, posn);
    }
  }

  /**
   * Represents a server in the Impala cluster. Each server is
   * assigned one or more fragments to execute. One server is the
   * coordinator.
s   */
  public static class ServerSynNode {
    private final String serverId;
    private boolean isCoordinator;
    private List<FragmentSynNode> fragments = new ArrayList<>();

    public ServerSynNode(String serverId) {
      this.serverId = serverId;
    }

    public void addFrag(FragmentSynNode frag) {
      fragments.add(frag);
      isCoordinator = frag.isRoot();
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

    public boolean isCoordinator() { return isCoordinator; }

    public String label() {
      if (isCoordinator) {
        return serverId + " (Coordinator)";
      } else {
        return serverId;
      }
    }
  }

  private final ProfileFacade profile;
  private FragmentSynNode[] fragments;
  private final OperatorSynNode[] operators;
  private RootFragSynNode rootFragment;
  private OperatorSynNode rootOperator;
  private final Map<String, ServerSynNode> servers = new HashMap<>();
  private List<String> serverNames = new ArrayList<>();

  /**
   * Constructor. Builds the DAG from the partially-analyzed profile.
   * Requires that the profile has parsed the query plan and the
   * fragment execution nodes. Builds the DAG from this partial
   * structure.
   *
   * @param profile The partially-analyzed query profile
   */
  public QueryDAG(ProfileFacade profile) {
    this.profile = profile;
    QueryPlan plan = profile.plan();

    operators = new OperatorSynNode[plan.operatorCount()];
    for (int i = 0; i < operators.length; i++) {
      operators[i] = new OperatorSynNode(plan.operator(i));
    }
    rootOperator = operators[plan.root().operatorId()];

    analyzeDag();
  }

  public OperatorSynNode[] operators() { return operators; }
  public OperatorSynNode rootOperator() { return rootOperator; }
  public RootFragSynNode rootFragment() { return rootFragment; }

  /**
   * Synthesizes the DAG from the structure within the query
   * profile.
   */
  public void analyzeDag() {
    ExecPNode exec = profile.exec();
    setCoordinator(exec.coordinator());
    defineFrags(exec.summaries());
    defineFragDetails(exec.details());
    defineServers();
    gatherOperatorSummaries();
    gatherOperatorDetails();
    stitchOperator(rootOperator);
    stitchFragmentOperator(rootOperator);
  }

  /**
   * Identify the coordinator fragment. This becomes the root of the
   * fragment DAG. The coordinator fragment is the highest-numbered
   * fragment, which tell us the number of fragments in the DAG.
   *
   * @param coordinator the coordinator fragment node, the one
   * labeled "Coordinator Fragment Fxx"
   */
  public void setCoordinator(FragmentPNode coordinator) {
    fragments = new FragmentSynNode[coordinator.fragmentId() + 1];
    rootFragment = new RootFragSynNode(coordinator,
        profile.summary().attrib(Attrib.SummaryAttrib.COORDINATOR));
    fragments[coordinator.fragmentId()] = rootFragment;
  }

  /**
   * Given the list of summary exec nodes (those labeled
   * "Averaged Fragment Fxx", define the set of fragments in the DAG.
   *
   * @param summaries fragment summary nodes
   */
  public void defineFrags(List<FragmentPNode> summaries) {
    for (FragmentPNode frag : summaries) {
      fragments[frag.fragmentId()] = new InternalFragSynNode(frag);
    }
  }

  /**
   * Given the set of fragment detail nodes (those labeled
   * "Fragment Fxx", assign the details to the previously-created
   * fragment synthesis node.
   *
   * @param details fragment details exec node which contains
   * a child for each server on which the fragment ran
   */
  public void defineFragDetails(List<AbstractFragPNode.FragInstancesPNode> details) {
    for (AbstractFragPNode.FragInstancesPNode fragDetails : details) {
      ((InternalFragSynNode) fragments[fragDetails.fragmentId()])
          .defineDetails(fragDetails);
    }
  }

  /**
   * Define the servers that make up the Impala cluster. Scans
   * all the fragment synthesis nodes, checking the server associated
   * with each fragment detail. Defines the server if not yet defined,
   * then adds that fragment to the server. Finally, creates a sorted
   * list of server names and marks the server which acts as the
   * coordinator.
   */
  private void defineServers() {
    defineServer(rootFragment.serverId(), rootFragment);
    for (int i = 0; i < fragments.length; i++) {
      FragmentSynNode fragSyn = fragments[i];
      if (fragSyn.isRoot()) { continue; }
      InternalFragSynNode internalSyn = (InternalFragSynNode) fragSyn;
      for (Entry<String, FragmentPNode> entry : internalSyn.details().entrySet()) {
        defineServer(entry.getKey(), fragSyn);
      }
    }
    serverNames.addAll(servers.keySet());
    Collections.sort(serverNames);
  }

  private void defineServer(String serverId, FragmentSynNode fragSyn) {
    ServerSynNode server = servers.get(serverId);
    if (server == null) {
      server = new ServerSynNode(serverId);
      servers.put(serverId, server);
    }
    server.addFrag(fragSyn);
  }

  /**
   * Operator summaries appear in the profile as a child of the fragment
   * in which the operator ran. Scan each fragment, and each operator in
   * that fragment. Assign the operator details to the operator synthesis
   * node.
   * <p>
   * The fragment uses a in-fix format: if an operator has only one input,
   * that input appears in the fragment's operator list. However, if the
   * operator has two inputs (a join), then the inputs appear as children
   * of the operator node with the left input first, the right input second.
   */
  private void gatherOperatorSummaries() {
    for (int i = 0; i < fragments.length; i++) {
      FragmentSynNode fragSyn = fragments[i];
      for (OperatorPNode operExec : fragSyn.averages().operators()) {
        gatherOperatorSummary(fragSyn, operExec);
      }
    }
  }

  /**
   * Define the summary for one operator within a fragment summary node.
   * Recursively descend into the children of the operator if this operator
   * is a join.
   * <p>
   * As part of this step, the fragment synthesis node is bound to the
   * operator synthesis node and visa-versa.
   *
   * @param fragSyn the fragment being processed
   * @param operExec the operator summary node being processed
   */
  private void gatherOperatorSummary(FragmentSynNode fragSyn, OperatorPNode operExec) {
    OperatorSynNode opSyn = operators[operExec.operatorId()];
    opSyn.setSummary(fragSyn, operExec);
    fragSyn.addOperator(opSyn);
    for (OperatorPNode child : operExec.children()) {
      gatherOperatorSummary(fragSyn, child);
    }
  }

  /**
   * Operators run on multiple servers. The details have been assigned
   * to the fragment synthesis node in a map. Using the map, iterate over
   * each fragment instance, then walk the fragment to assign the operator
   * details to the operator synthesis node. Again, this is a recursive
   * operation as explained for
   * {@link #gatherOperatorSummary(FragmentSynNode, OperatorPNode)}.
   */
  private void gatherOperatorDetails() {
    for (int i = 0; i < fragments.length; i++) {
      FragmentSynNode fragSyn = fragments[i];
      if (fragSyn.isRoot()) { continue; }
      InternalFragSynNode internalSyn = (InternalFragSynNode) fragSyn;
      for (Entry<String, FragmentPNode> entry : internalSyn.details().entrySet()) {
        for (OperatorPNode operExec : entry.getValue().operators()) {
          gatherOperatorDetails(entry.getKey(), operExec);
        }
      }
    }
  }

  /**
   * Gather operator details for one (server, operator) pair. Recursively
   * descend into children for this operator in the fragment profile node,
   * which are those that ran in the same fragment.
   *
   * @param serverId server on which the operator ran
   * @param operExec the operator details profile node
   */
  private void gatherOperatorDetails(String serverId, OperatorPNode operExec) {
    OperatorSynNode opSyn = operators[operExec.operatorId()];
    opSyn.addDetail(serverId, operExec);
    for (OperatorPNode child : operExec.children()) {
      gatherOperatorDetails(serverId, child);
    }
  }

  /**
   * Mirror the operator plan DAG into the operator synthesis nodes,
   * starting at the subtree given by one node. Uses the plan node
   * for this operator to get the list of child plan nodes. Uses the
   * operator ID in the child plan node to find the matching operator
   * synthesis node. Then, builds the parent/child relations in the
   * operator synthesis nodes. Recursively applies this process to
   * child operators.
   *
   * @param opSyn operator synthesis node to be stitched
   */
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

  /**
   * Identify the operator which acts as the root for a particular
   * fragment. An operator is a fragment root if the operator is
   * the root, or its parent is in a different fragment. Mark each
   * fragment with its root operator.
   *
   * @param opSyn operator synthesis node to be stitched
   */
  private void stitchFragmentOperator(OperatorSynNode opSyn) {
    if (opSyn.isFragmentRoot()) {
      opSyn.fragment().setRootOperator(opSyn);
    }
    if (opSyn.isLeaf()) { return; }
    for (OperatorSynNode child : opSyn.children()) {
      stitchFragmentOperator(child);
    }
  }

  /**
   * Prints the overall DAG structure to verify correctness.
   * Not intended to print analysis details; that should be done
   * using an ad-hoc mechanism for each analysis.
   */
  public void print() {
    AttribPrintFormatter fmt = new AttribPrintFormatter();
    format(fmt);
  }

  public void format(AttribFormatter fmt) {
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
