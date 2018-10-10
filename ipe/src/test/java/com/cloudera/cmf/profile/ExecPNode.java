package com.cloudera.cmf.profile;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.cmf.profile.AbstractFragPNode.FragInstancesPNode;
import com.cloudera.cmf.profile.AbstractFragPNode.FragmentPNode;

public class ExecPNode extends ProfileNode {

  public boolean expanded;
  private FragmentPNode coordinator;
  private final List<FragmentPNode> summaries = new ArrayList<>();
  private final List<FragInstancesPNode> details = new ArrayList<>();

  public ExecPNode(NodeIterator nodeIndex) {
    super(nodeIndex, PNodeType.EXEC);
  }

  public void expand(ProfileFacade analyzer) {
    if (expanded) { return; }
    NodeIterator nodeIndex = new ProfileNode.NodeIterator(analyzer.profile(), index + 1);
    for (int i = 0; i < childCount(); i++) {
      PNodeType childType = PNodeType.parse(nodeIndex.node().getName());
      switch (childType) {
      case COORD:
        coordinator = new FragmentPNode(nodeIndex, childType);
        break;
      case FRAG_SUMMARY:
        summaries.add(new FragmentPNode(nodeIndex, childType));
        break;
      case FRAG_DETAIL:
        details.add(new FragInstancesPNode(nodeIndex));
        break;
      default:
        throw new IllegalStateException("Exec node type");
      }
    }
    expanded = true;
  }

  public FragmentPNode coordinator() { return coordinator; }
  public List<FragmentPNode> summaries() { return summaries; }
  public List<FragInstancesPNode> details() { return details; }

  @Override
  public String genericName() { return "Execution Profile"; }

  @Override
  public PNodeType nodeType() { return PNodeType.EXEC; }

  @Override
  public List<ProfileNode> childNodes() {
    List<ProfileNode> children = new ArrayList<>();
    children.add(coordinator);
    children.addAll(summaries);
    children.addAll(details);
    return children;
  }
}
