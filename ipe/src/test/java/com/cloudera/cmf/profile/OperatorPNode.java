package com.cloudera.cmf.profile;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.thrift.TRuntimeProfileNode;

import com.google.common.base.Preconditions;

/**
 * Represents execution-time detail about an operator.
 *
 * <pre>
 * &lt;operator>_NODE (id=xx)
 *   &lt;operator>_NODE (id=xx)
 *   ...
 *   filter
 *   ...
 * </pre>
 *
 * The child operators represent the inputs to an operator.
 * Most take a single input, joins take two.
 * <p>
 * Two kinds of AGGREGATION nodes appear: STREAMING and FINALIZE.
 * It is hard to differentiate them here, use the plan to tell them
 * apart. Use the proper counters in {@link Attrib} depending on
 * the aggregate type.
 */
public class OperatorPNode extends ProfileNode {

  public enum OperType {
    EXCHANGE,
    AGG,
    HASH_JOIN,
    HDFS_SCAN
  }

  private final String operatorName;
  private final int operatorIndex;
  private final OperType operType;
  private List<OperatorPNode> children = new ArrayList<>();
  private final List<ProfileNode> filters = new ArrayList<>();

  public OperatorPNode(OperatorPNode copyFrom) {
    super(copyNode(copyFrom.node()), copyFrom.nodeType());
    operatorName = copyFrom.operatorName;
    operatorIndex = copyFrom.operatorIndex;
    operType = copyFrom.operType;
  }

  public OperatorPNode(NodeIterator nodeIndex, PNodeType nodeType) {
    super(nodeIndex, nodeType);
    Pattern p = Pattern.compile("(.*)_NODE \\(id=(\\d+)\\)");
    Matcher m = p.matcher(name());
    Preconditions.checkState(m.matches());
    operatorName = m.group(1);
    operatorIndex = Integer.parseInt(m.group(2));
    operType = nodeType.operType();
    for (int i = 0; i < childCount(); i++) {
      TRuntimeProfileNode profileNode = nodeIndex.node();
      String name = profileNode.getName();
      PNodeType childType = PNodeType.parse(name);
      if (childType == PNodeType.FILTER) {
        filters.add(new ProfileNode(nodeIndex, PNodeType.FILTER));
      } else {
        children.add(new OperatorPNode(nodeIndex, childType));
      }
    }
  }

  public int operatorId() { return operatorIndex; }
  public OperType operType() { return operType; }
  public List<OperatorPNode> children() { return children; }

  @Override
  public List<ProfileNode> childNodes() {
    List<ProfileNode> children = new ArrayList<>();
    children.addAll(children);
    children.addAll(filters);
    return children;
  }
}