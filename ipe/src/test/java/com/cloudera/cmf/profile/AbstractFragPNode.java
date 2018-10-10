package com.cloudera.cmf.profile;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.thrift.TRuntimeProfileNode;

import com.cloudera.cmf.profile.ParseUtils.FragmentInstance;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Parent class for the four kinds of fragment nodes:
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
public abstract class AbstractFragPNode extends ProfileNode {

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
  public static class FragInstancesPNode extends AbstractFragPNode {

    protected final List<FragmentPNode> fragments = new ArrayList<>();

    public FragInstancesPNode(NodeIterator nodeIndex) {
      super(nodeIndex, PNodeType.FRAG_DETAIL,
          ParseUtils.parseFragmentId(nodeIndex.node().getName()));
      for (int i = 0; i < childCount(); i++) {
        FragmentPNode frag = new FragmentPNode(
            nodeIndex, fragmentId);
        fragments.add(frag);
      }
    }

    public List<FragmentPNode> hostNodes() {
      return fragments;
    }

    @Override
    public List<ProfileNode> childNodes() {
      return Lists.newArrayList(fragments);
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
  public static class FragmentPNode extends AbstractFragPNode {

    public enum FragmentType {
      COORDINATOR,
      AVERAGED,
      INSTANCE
    }

    private final FragmentType fragmentType;
    protected FragmentInstance instanceId;
    private ProfileNode blockMgr;
    private ProfileNode codeGen;
    private ProfileNode dataStreamSender;
    // TODO: This is probably the root operator
    private final List<OperatorPNode> operators = new ArrayList<>();

    public FragmentPNode(NodeIterator nodeIndex, PNodeType nodeType) {
      super(nodeIndex, nodeType,
          ParseUtils.parseFragmentId(nodeIndex.node().getName()));
      instanceId = null;
      switch(nodeType) {
      case COORD:
        fragmentType = FragmentType.COORDINATOR;
        break;
      case FRAG_SUMMARY:
        fragmentType = FragmentType.AVERAGED;
        break;
      default:
        throw new IllegalStateException("Frament type: " + nodeType);
      }
      parseChildren(nodeIndex);
    }

    public FragmentPNode(NodeIterator nodeIndex, int fragmentId) {
      super(nodeIndex, PNodeType.FRAG_INSTANCE, fragmentId);
      fragmentType = FragmentType.INSTANCE;
      instanceId = new FragmentInstance(name());
      parseChildren(nodeIndex);
    }

    private void parseChildren(NodeIterator nodeIndex) {
      Preconditions.checkState(nodeIndex.index() == index + 1);
      for (int i = 0; i < childCount(); i++) {
        TRuntimeProfileNode profileNode = nodeIndex.node();
        String name = profileNode.getName();
        parseChild(nodeIndex, name);
        nodeIndex.incr();
      }
    }

    protected void parseChild(NodeIterator nodeIndex, String name) {
      PNodeType nodeType = PNodeType.parse(name);
      switch (nodeType) {
      case BLOCK_MGR:
        blockMgr = new ProfileNode(nodeIndex, nodeType);
        break;
      case CODE_GEN:
         codeGen = new ProfileNode(nodeIndex, nodeType);
         break;
      case SENDER:
        dataStreamSender = new ProfileNode(nodeIndex, nodeType);
        break;
      default:
        operators.add(new OperatorPNode(nodeIndex, nodeType));
      }
    }

    public List<OperatorPNode> operators() { return operators; }

    public FragmentType fragmentType() { return fragmentType; }

    public String serverId() {
      return instanceId == null ? null : instanceId.serverId();
    }

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

  protected final int fragmentId;

  public AbstractFragPNode(NodeIterator nodeIndex, PNodeType nodeType, int fragmentId) {
    super(nodeIndex, nodeType);
    this.fragmentId = fragmentId;
  }

  public int fragmentId() { return fragmentId; }
}
