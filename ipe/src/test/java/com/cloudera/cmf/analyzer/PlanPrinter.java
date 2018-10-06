package com.cloudera.cmf.analyzer;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

import com.cloudera.cmf.analyzer.QueryPlan.PlanNode;

public class PlanPrinter {

  private final QueryPlan plan;
  private PrintWriter out;
  private int indent;
  private int prevIndent;

  public PlanPrinter(ProfileAnalyzer profile) {
    this.plan = profile.query().plan();
  }

  public PlanPrinter toStdOut() {
    out = new PrintWriter(
        new OutputStreamWriter(System.out), true);
    return this;
  }

  public void print() {
    visitNode(plan.root());
  }

  private void visitNode(PlanNode node) {
    node.print(this);
    List<PlanNode> children = node.children();
    if (children == null) { return; }
    int oldIndent = indent;
    for (int i = children.size() - 1; i >= 0;  i--) {
      indent = oldIndent + i;
      visitNode(children.get(i));
    }
    indent = oldIndent;
  }

  public void writeIndent() {
    prevIndent = Math.min(prevIndent, indent);
    for (int i = 0; i < prevIndent; i++) {
      out.print("|  ");
    }
    if (prevIndent < indent) {
      out.print("|- ");
    }
    prevIndent = indent;
  }

  public void write(String text) {
    writeIndent();
    out.println(text);
  }

  public void writeBlock(String text) {
    String lines[] = text.split("\n");
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      if (line.isEmpty()) { continue; }
      writeIndent();
      out.print("  ");
      out.println(line);
    }
  }
}
