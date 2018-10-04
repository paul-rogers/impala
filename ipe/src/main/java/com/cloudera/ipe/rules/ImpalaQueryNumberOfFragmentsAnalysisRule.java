// Copyright (c) 2017 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileCounter;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileNode;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.rules.AttributeMetadata;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaQueryNumberOfFragmentsAnalysisRule implements ImpalaAnalysisRule {
  private static Logger LOG = LoggerFactory.getLogger(
		  ImpalaQueryNumberOfFragmentsAnalysisRule.class);
  public static final String NUMBER_FRAGMENTS = "num_fragments";
  public static final String NUMBER_BACKENDS = "num_backends";

  // Counter name in Impala query profile
  public static final String NUMBER_FRAGMENTS_INSTANCES_COUNTER_NAME = "NumFragmentInstances";
  public static final String NUMBER_BACKENDS_COUNTER_NAME = "NumBackends";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);

    long numberOfFragments = -1;
    long numberOfBackends = -1;

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
    ImpalaRuntimeProfileNode executionNode = tree.getExecutionProfileNode();

    if (executionNode == null) {
      return b.build();
    }

    try {
      Iterator<ImpalaRuntimeProfileCounter> it = executionNode.getRootCounters().iterator();

      while(it.hasNext()) {
        ImpalaRuntimeProfileCounter counter = it.next();

        if (counter.getCounter().getName().equals(NUMBER_FRAGMENTS_INSTANCES_COUNTER_NAME)) {
          numberOfFragments = counter.getCounter().value;
          continue;
        }

        if (counter.getCounter().getName().equals(NUMBER_BACKENDS_COUNTER_NAME)) {
          numberOfBackends = counter.getCounter().value;
        }

      }
    } catch (Exception e) {
      LOG.warn(String.format("Failed to parse Counters %1$s and %2$s,",
          " full stack trace follows:%3$s.",
          NUMBER_FRAGMENTS_INSTANCES_COUNTER_NAME, NUMBER_BACKENDS_COUNTER_NAME), e);
    }

    if (numberOfFragments != -1) {
      b.put(NUMBER_FRAGMENTS, String.valueOf(numberOfFragments));
    }

    if (numberOfBackends != -1 ) {
      b.put(NUMBER_BACKENDS, String.valueOf(numberOfBackends));
    }

    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    AttributeMetadata numberOfFragments = AttributeMetadata.newBuilder()
        .setName(NUMBER_FRAGMENTS)
        .setDisplayNameKey("impala.analysis.number_fragments.name")
        .setDescriptionKey("impala.analysis.number_fragments.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata numberOfBackends = AttributeMetadata.newBuilder()
        .setName(NUMBER_BACKENDS)
        .setDisplayNameKey("impala.analysis.number_backends.name")
        .setDescriptionKey("impala.analysis.number_backends.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    return ImmutableList.of(
      numberOfFragments,
      numberOfBackends);
  }
}
