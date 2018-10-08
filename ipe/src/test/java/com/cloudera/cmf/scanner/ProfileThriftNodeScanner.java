package com.cloudera.cmf.scanner;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.thrift.TRuntimeProfileNode;

import com.cloudera.cmf.printer.AttribFormatter;
import com.cloudera.cmf.printer.AttribPrintFormatter;
import com.cloudera.cmf.profile.PNodeType;
import com.cloudera.cmf.profile.ParseUtils;
import com.cloudera.cmf.profile.ParseUtils.FragmentInstance;

import com.cloudera.cmf.profile.ProfileFacade;
import com.cloudera.cmf.scanner.ProfileScanner.Action;

public class ProfileThriftNodeScanner {

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
    void reset(Context context);
    Level level();
    PNodeType target();
    void apply(Context context);
    void finish(Context context);
  }

  public static abstract class AbstractRule implements Rule {

    protected Level level = Level.ALL;
    protected PNodeType target;

    @Override
    public void reset(Context context) { }
    @Override
    public Level level() { return level; }
    @Override
    public PNodeType target() { return target; }
    @Override
    public void finish(Context context) { }
  }

  public static class ThriftNodeScanAction implements Action {

    private final ProfileThriftNodeScanner scanner;

    public ThriftNodeScanAction(ProfileThriftNodeScanner scanner) {
      this.scanner = scanner;
    }

    public ThriftNodeScanAction() {
      this(new ProfileThriftNodeScanner());
    }

    @Override
    public void apply(ProfileFacade profile) {
      scanner.apply(profile);
    }

    public ThriftNodeScanAction add(Rule rule) {
      scanner.add(rule);
      return this;
    }
  }

  public static class Context {
    protected final AttribFormatter fmt;
    protected final ProfileFacade profile;
    protected State state;
    protected int posn;
    protected TRuntimeProfileNode node;
    protected PNodeType nodeType;
    protected int fragmentId;
    protected FragmentInstance instanceId;

    public Context(ProfileFacade profile, AttribFormatter fmt) {
      this.profile = profile;
      this.fmt = fmt;
      state = State.ROOT;
    }

    public ProfileFacade profile() { return profile; }
    public State state() { return state; }
    public int posn() { return posn - 1; }
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

  protected final AttribFormatter fmt;
  public List<Rule> rules = new ArrayList<>();

  public ProfileThriftNodeScanner() {
    this(new AttribPrintFormatter());
  }

  public ProfileThriftNodeScanner(AttribFormatter fmt) {
    this.fmt = fmt;
  }

  public ProfileThriftNodeScanner add(Rule rule) {
    rules.add(rule);
    return this;
  }

  public void apply(ProfileFacade profile) {
    Context context = new Context(profile, fmt);
    for (Rule rule : rules) {
      rule.reset(context);
    }
    visit(context);
    for (Rule rule : rules) {
      rule.finish(context);
    }
  }

  private void visit(Context context) {
    context.node = context.profile.node(context.posn++);
    String nodeName = context.node.getName();
    context.nodeType = PNodeType.parse(nodeName);
    switch (context.nodeType) {
    case FRAG_SUMMARY:
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
    for (Rule rule : rules) {
      applyRule(context, rule);
    }
    int childCount = context.node.getNum_children();
    for (int i = 0; i < childCount; i++) {
      visit(context);
    }
  }

  private void applyRule(Context context, Rule rule) {
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

    rule.apply(context);
  }

}
