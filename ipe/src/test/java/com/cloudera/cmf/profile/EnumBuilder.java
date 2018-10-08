package com.cloudera.cmf.profile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TTimeSeriesCounter;

/**
 * Development-time helper class to generate enums for profile
 * node attributes and counters. Uses an actual profile as
 * the source of truth, sorts nodes by type, verifies that
 * all nodes of the same type have the same attributes, then
 * generates enums. Note that some nodes have attributes that
 * vary such as "Filter x ...".
 */

public class EnumBuilder {

  public static class NodeType {
    String name;
    List<ProfileNode> examples = new ArrayList<>();
    List<String> attribNames = new ArrayList<>();
    List<String> counterNames = new ArrayList<>();
    ProfileNode counterPrototype;
    List<String> timeSeriesNames = new ArrayList<>();
    ProfileNode timeSeriesPrototype;

    public NodeType(String name) {
      this.name = name;
    }

    public void analyze() {
//      analyzeAttribs(); // Done
//      analyzeCounters(); // Done
      analyzeTimeSeries();
    }

    public void analyzeAttribs() {
      for (ProfileNode example : examples) {
        TRuntimeProfileNode node = example.node();
        List<String> nodeAttribs = node.getInfo_strings_display_order();
        if (nodeAttribs.isEmpty()) { continue; }
        if (attribNames.isEmpty()) {
          attribNames.addAll(nodeAttribs);
        } else if (! attribNames.equals(nodeAttribs)) {
          System.out.println(name + " Conflicting display order");
          System.out.println("  " + attribNames.toString());
          System.out.println("  " + nodeAttribs.toString());
          if (nodeAttribs.size() > attribNames.size()) {
            attribNames.clear();
            attribNames.addAll(nodeAttribs);
          }
        }
      }
    }

    public void analyzeCounters() {
      for (ProfileNode example : examples) {
        TRuntimeProfileNode node = example.node();
        if (node.getCountersSize() == 0) { continue; }
        List<String> nodeNames = new ArrayList<>();
        for (TCounter counter : node.getCounters()) {
          nodeNames.add(counter.getName());
        }
        if (counterNames.isEmpty()) {
          counterNames.addAll(nodeNames);
          counterPrototype = example;
        } else if (! counterNames.equals(nodeNames)) {
          System.out.println(name + " Conflicting counter names");
          System.out.println("  " + counterNames.toString());
          System.out.println("  " + nodeNames.toString());
          if (nodeNames.size() > counterNames.size()) {
            counterNames.clear();
            counterNames.addAll(nodeNames);
          }
        }
      }
    }

    public void analyzeTimeSeries() {
      for (ProfileNode example : examples) {
        TRuntimeProfileNode node = example.node();
        if (node.getTime_series_countersSize() == 0) { continue; }
        List<String> nodeNames = new ArrayList<>();
        for (TTimeSeriesCounter counter : node.getTime_series_counters()) {
          nodeNames.add(counter.getName());
        }
        if (timeSeriesNames.isEmpty()) {
          timeSeriesNames.addAll(nodeNames);
          timeSeriesPrototype = example;
        } else if (! timeSeriesNames.equals(nodeNames)) {
          System.out.println(name + " Conflicting time series names");
          System.out.println("  " + timeSeriesNames.toString());
          System.out.println("  " + nodeNames.toString());
          if (nodeNames.size() > timeSeriesNames.size()) {
            timeSeriesNames.clear();
            timeSeriesNames.addAll(nodeNames);
          }
        }
      }
    }

    public void print() {
      System.out.println("==== " + name + " ====");
//      printAttribs(); // Done
//      printCounters(); // Done
      printTimeSeriesCounters();
    }

    @SuppressWarnings("unused")
    private void printAttribs() {
      if (! attribNames.isEmpty()) {
        System.out.println("-- Attribs --");
        generateAttribs(attribNames);
      }
    }

    @SuppressWarnings("unused")
    private void printCounters() {
      System.out.println("// Generated using EnumBuilder");
      System.out.println("public enum Counter {");
      int i = 0;
      int count = counterPrototype.node().getCountersSize();
      for (TCounter counter : counterPrototype.node().getCounters()) {
        String key = counter.getName();
        String itemName = clean(key).toUpperCase();
        String term = (++i == count) ? ";" : ",";
        System.out.println(String.format("  %s(\"%s\", TUnit.%s)%s",
            itemName, key, counter.getUnit().name(), term));
      }
      System.out.println("}");
    }

    private void printTimeSeriesCounters() {
      if (timeSeriesPrototype == null) { return; }
      System.out.println("// Generated using EnumBuilder");
      System.out.println("public enum TimeSeries {");
      int i = 0;
      int count = timeSeriesPrototype.node().getTime_series_countersSize();
      for (TTimeSeriesCounter counter : timeSeriesPrototype.node().getTime_series_counters()) {
        String key = counter.getName();
        String itemName = clean(key).toUpperCase();
        String term = (++i == count) ? ";" : ",";
        System.out.println(String.format("  %s(\"%s\", TUnit.%s)%s",
            itemName, key, counter.getUnit().name(), term));
      }
      System.out.println("}");
    }

    public static void generateAttribs(TRuntimeProfileNode node) {
      generateAttribs(node.getInfo_strings_display_order());
    }

    /**
     * Development method to generate enum names from observed
     * keys in the "info" field order list.
     * @param node the node to parse
     */
    public static void generateAttribs(List<String> attribNames) {
      generateEnum("Attrib", attribNames);
    }

    public static void generateEnum(String enumName, List<String> itemNames) {
      System.out.println("// Generated using EnumBuilder");
      System.out.println("public enum " + enumName + " {");
      int count = itemNames.size();
      for (int i = 0; i < count; i++) {
        String key = itemNames.get(i);
        String itemName = clean(key).toUpperCase();
        String term = (i + 1 == count) ? ";" : ",";
        System.out.println(String.format("  %s(\"%s\")%s",
            itemName, key, term));
      }
      System.out.println("}");
    }

    public static String clean(String name) {
      name = name.replaceAll("[ \\-()<>]", "_");
      name = name.replaceAll("__", "_");
      name = name.replaceAll("_$", "");
      name = name.replaceAll("([a-z])([A-Z])", "$1_$2");
      return name;
    }
  }

  private ProfileFacade profile;
  Map<String,NodeType> nodeTypes = new HashMap<>();

  public EnumBuilder(ProfileFacade profile) {
    this.profile = profile;
  }

  public void build() {
    gatherExamples(profile.exec());
    for (NodeType nodeType : nodeTypes.values()) {
      nodeType.analyze();
    }
  }

  private void gatherExamples(ProfileNode node) {
     addExample(node);
     for (ProfileNode child : node.childNodes()) {
       gatherExamples(child);
     }
  }

  private void addExample(ProfileNode node) {
    String name = node.genericName();
    // Special case: two kinds of aggregates
    if (name.equals("AGGREGATION")) {
      if (node.node().getCounters().get(0).name.equals("GetNewBlockTime")) {
        name += " -Streaming";
      }
      else {
        name += " - Finalize";
      }
    }
    NodeType nodeType = nodeTypes.get(name);
    if (nodeType == null) {
      nodeType = new NodeType(name);
      nodeTypes.put(name, nodeType);
    }
    nodeType.examples.add(node);
  }

  public void print() {
    for (NodeType nodeType : nodeTypes.values()) {
      nodeType.print();
    }
  }
}
