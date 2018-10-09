package com.cloudera.cmf.scanner;

import com.cloudera.cmf.printer.AttribFormatter;
import com.cloudera.cmf.profile.ProfileNode;
import com.cloudera.cmf.scanner.ProfileThriftNodeScanner.AbstractRule;
import com.cloudera.cmf.scanner.ProfileThriftNodeScanner.Context;
import com.cloudera.cmf.scanner.ProfileThriftNodeScanner.CounterRollUp;
import com.cloudera.cmf.scanner.ProfileThriftNodeScanner.Level;

public class ThriftNodeRules {

  public static class DisabledCodeGenRule extends AbstractRule {

     public DisabledCodeGenRule() {
      level = Level.DETAIL;
    }

     @Override
     public RollUp startGroup() {
        return new CounterRollUp();
     }

    @Override
    public void apply(Context context, RollUp rollup) {
      if (! context.nodeType().isOperator()) { return; }
      String option = ProfileNode.getAttrib(context.node, "ExecOption");
      if (option == null) { return; };
      if (! option.contains("Codegen Disabled")) { return; }
      CounterRollUp counter = (CounterRollUp) rollup;
      counter.incr();
      if (counter.count() > 1) { return; }
      context.log("Codegen Disabled", option.trim());
    }

    @Override
    public void finishGroup(AttribFormatter fmt, RollUp rollup) {
      CounterRollUp counter = (CounterRollUp) rollup;
      if (counter.count() == 0) { return; }
      fmt.attrib("Codegen Disabled Count", counter.count());

    }

  }
}
