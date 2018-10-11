package com.cloudera.cmf.scanner;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.thrift.TRuntimeProfileNode;

import com.cloudera.cmf.printer.AttribFormatter;
import com.cloudera.cmf.profile.PNodeType;
import com.cloudera.cmf.profile.ParseUtils;
import com.cloudera.cmf.profile.ParseUtils.FragmentInstance;
import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.profile.ProfileNode.NodeIterator;
import com.cloudera.cmf.scanner.ProfileScanner.Action;

public class ProfileThriftNodeScanner implements Action {

  public enum Level {
    ALL,
    SUMMARY,
    DETAIL
  }

  public enum State {
    ROOT,
    SUMMARY,
    DETAIL
  }

  public interface Rule {
    RollUp startGroup();
    Level level();
    PNodeType target();
    void apply(Context context, RollUp profileRollup);
    void finishGroup(AttribFormatter fmt, RollUp rollup);
  }

  public static class NullRollUp implements RollUp {

    @Override
    public void add(RollUp detail) { }
  }

  public static class CounterRollUp implements RollUp {

    private int count;

    @Override
    public void add(RollUp detail) {
      count += ((CounterRollUp) detail).count;
    }

    public void add(int n) { count += n; }
    public void incr() { count++; }
    public int count() { return count; }
  }

  public static abstract class AbstractRule implements Rule {

    protected Level level = Level.ALL;
    protected PNodeType target;

    @Override
    public RollUp startGroup() {
      return new NullRollUp();
    }

    @Override
    public Level level() { return level; }
    @Override
    public PNodeType target() { return target; }

    @Override
    public void finishGroup(AttribFormatter fmt, RollUp rollup) { }
  }

  public static abstract class SimpleRule extends AbstractRule {

    @Override
    public RollUp startGroup() {
      return new CounterRollUp();
    }

    @Override
    public void apply(Context context, RollUp rollup) {
      if (apply(context)) {
        ((CounterRollUp) rollup).incr();
      }
    }

    public abstract boolean apply(Context context);
  }

  public static class Context {
    protected final AttribFormatter fmt;
    protected final ProfileFacade profile;
    protected State state;
    protected NodeIterator nodeIndex;
    protected TRuntimeProfileNode node;
    protected PNodeType nodeType;
    protected int fragmentId;
    protected FragmentInstance instanceId;
    protected RollUp totals[];

    public Context(ProfileFacade profile, AttribFormatter fmt) {
      this.profile = profile;
      this.fmt = fmt;
      state = State.ROOT;
    }

    public ProfileFacade profile() { return profile; }
    public State state() { return state; }
    public int posn() { return nodeIndex.index() - 1; }
    public TRuntimeProfileNode node() { return node; }
    public PNodeType nodeType() { return nodeType; }
    public int fragentId() { return fragmentId; }
    public FragmentInstance fragmentInstance() { return instanceId; }

    public void log(String msg, String detail) {
      fmt.startGroup(msg);
      if (detail != null) {
        fmt.attrib("Detail", detail);
      }
      fmt.attrib("Location", profile.label());
      fmt.attrib("Query ID", profile.queryId());
      fmt.attrib("State", state.name());
      fmt.attrib("Node Posn", posn());
      fmt.attrib("Node Type", nodeType().name());
      fmt.attrib("Node Name", node().getName());
      if (fragmentId != 0) {
        fmt.attrib("Fragment ID", fragmentId);
      }
      if (instanceId != null) {
        fmt.attrib("Fragment Instance", instanceId.fragmentGuid());
        fmt.attrib("Server ID", instanceId.serverId());
      }
      fmt.endGroup();
    }

    public void summary(String ruleName, String detail) {
      fmt.startGroup(ruleName);
      fmt.attrib("Location", profile.label());
      fmt.attrib("Summary", detail);
      fmt.endGroup();
    }
  }

  protected AttribFormatter fmt;
  public List<Rule> rules = new ArrayList<>();

  @Override
  public void bindFormatter(AttribFormatter fmt) {
    this.fmt = fmt;
  }

  public ProfileThriftNodeScanner add(Rule rule) {
    rules.add(rule);
    return this;
  }

  @Override
  public void apply(ProfileFacade profile) {
    Context context = new Context(profile, fmt);
    context.nodeIndex = new NodeIterator(profile.profile(), 0);
    context.totals = new RollUp[rules.size()];
    for (int i = 0; i < rules.size(); i++) {
      context.totals[i] = rules.get(i).startGroup();
    }
    visit(context);
    fmt.startGroup("Summary");
    for (int i = 0; i < rules.size(); i++) {
      rules.get(i).finishGroup(fmt, context.totals[i]);
    }
    fmt.endGroup();
  }

  private void visit(Context context) {
    context.node = context.nodeIndex.next();
    String nodeName = context.node.getName();
    context.nodeType = PNodeType.parse(nodeName);
    switch (context.nodeType) {
    case FRAG_AVERAGE:
      context.state = State.SUMMARY;
      context.fragmentId = ParseUtils.parseFragmentId(nodeName);
      visitNode(context);
      context.state = State.ROOT;
      context.fragmentId = 0;
      break;
    case FRAG_DETAIL:
      context.state = State.DETAIL;
      context.fragmentId = ParseUtils.parseFragmentId(nodeName);
      visitNode(context);
      context.state = State.ROOT;
      context.fragmentId = 0;
      break;
    case FRAG_INSTANCE:
      context.instanceId = new FragmentInstance(nodeName);
      visitNode(context);
      context.instanceId = null;
      break;
    default:
      visitNode(context);
    }
  }

  private void visitNode(Context context) {
    for (int i = 0; i < rules.size(); i++) {
      applyRule(context, rules.get(i), context.totals[i]);
    }
    int childCount = context.node.getNum_children();
    for (int i = 0; i < childCount; i++) {
      visit(context);
    }
  }

  private void applyRule(Context context, Rule rule, RollUp totals) {
    switch (rule.level()) {
    case ALL:
      break;
    case DETAIL:
      if (context.state != State.DETAIL) { return; }
      break;
    case SUMMARY:
      if (context.state != State.SUMMARY) { return; }
      break;
    default:
      throw new IllegalStateException("Level: " + rule.level());
    }

    PNodeType target = rule.target();
    if (target != null && target != context.nodeType) {
      return;
    }

    rule.apply(context, totals);
  }

}
