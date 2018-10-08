package com.cloudera.cmf.printer;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

import com.cloudera.cmf.analyzer.ProfileFacade;
import com.cloudera.cmf.analyzer.QueryPlan;
import com.cloudera.cmf.analyzer.QueryPlan.PlanNode;

public class PlanPrinter {

  private final QueryPlan plan;
  private PrintWriter out;
  private int level;
  private int prevLevel;
  private boolean isLeaf;

  public PlanPrinter(ProfileFacade profile) {
    this.plan = profile.plan();
  }

  public PlanPrinter toStdOut() {
    out = new PrintWriter(
        new OutputStreamWriter(System.out), true);
    return this;
  }

  public void print() {
    visitNode(plan.root());
    out.flush();
  }

  private void visitNode(PlanNode node) {
    List<PlanNode> children = node.children();
    isLeaf = (children == null);
    node.print(this);
    if (isLeaf) { return; }
    int oldLevel = level;
    for (int i = children.size() - 1; i >= 0;  i--) {
      level = oldLevel + i;
      visitNode(children.get(i));
    }
    level = oldLevel;
  }

  public void writeIndent() {
    int n = Math.min(prevLevel, level);
    for (int i = 0; i < n; i++) {
      out.print("|  ");
    }
    if (prevLevel < level) {
      out.print("+- ");
    }
    prevLevel = level;
  }

  public void write(String text) {
    writeIndent();
    out.println(text);
  }

  public void writeBlockIndent() {
    out.print(detailsPrefix());
  }

  public void writeDetail(String line) {
    writeBlockIndent();
    out.println(line);
  }

  public void writeBlock(String text) {
    String lines[] = text.split("\n");
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      if (line.isEmpty()) { continue; }
      writeDetail(line);
    }
  }

  public String detailsPrefix() {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < level; i++) {
      buf.append("|  ");
    }
    if (isLeaf) {
      buf.append("   ");
    } else {
      buf.append("|  ");
    }
    return buf.toString();
  }

  public void writePreFormatted(String text) {
    out.print(text);
  }

  public void endDetails() {
    writeDetail("");
  }
}
