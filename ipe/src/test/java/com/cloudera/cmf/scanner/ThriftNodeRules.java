package com.cloudera.cmf.scanner;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.cloudera.cmf.profile.ProfileNode;
import com.cloudera.cmf.scanner.ProfileThriftNodeScanner.AbstractRule;
import com.cloudera.cmf.scanner.ProfileThriftNodeScanner.Context;
import com.cloudera.cmf.scanner.ProfileThriftNodeScanner.Level;

public class ThriftNodeRules {

  public static class DisabledCodeGenRule extends AbstractRule {

    private final Map<String, Integer> msgs = new HashMap<>();

    public DisabledCodeGenRule() {
      level = Level.DETAIL;
    }

    @Override
    public void reset(Context context) {
      msgs.clear();
    }

    @Override
    public void apply(Context context) {
      if (! context.nodeType().isOperator()) {
        return;
      }
      String option = ProfileNode.getAttrib(context.node, "ExecOption");
      if (option == null) { return; };
      Integer count = msgs.get(option);
      if (count == null) {
        context.log("Codegen Disabled", option.trim());
        count = 1;
      } else {
        count = count + 1;
      }
      msgs.put(option, count);
    }

    @Override
    public void finish(Context context) {
      for (Entry<String, Integer> entry : msgs.entrySet()) {
        int count = entry.getValue();
        if (count == 1) { continue; }
        context.summary("Disabled Codegen",
            entry.getKey() + " occurred " + count + " times");
      }
    }

  }
}
