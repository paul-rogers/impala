// Copyright (c) 2016 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import org.apache.impala.thrift.TCounter;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.util.ImpalaRuntimeProfileUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * This analysis rule is responsible for extracting information on the memory
 * spilled.
 */
public class ImpalaMemorySpilledAnalysisRule implements ImpalaAnalysisRule {

  public static final String MEMORY_SPILLED = "memory_spilled";

  // In older versions of Impala (up to 2.9) the bytes spilled to disk is tracked under
  // the "BlockMgr" profile, which appears under a single (arbitrarily chosen) fragment
  // instance per Impala daemon. The counter has different names in different versions.
  private static final String BLOCK_MGR_PROFILE_PREFIX = "BlockMgr";
  private static final ImmutableSet<String> BLOCK_MGR_COUNTERS =
      ImmutableSet.of("BytesWritten", "ScratchBytesWritten");

  // In newer versions of Impala (2.10+) the bytes spilled to disk is tracked per query
  // operator under a "Buffer pool:" sub-profile.
  private static final String BUFFER_POOL_PROFILE_PREFIX = "Buffer pool";
  private static final String BUFFER_POOL_COUNTER = "WriteIoBytes";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    List<TCounter> counters = Lists.newArrayList();
    tree.collectCountersWithName(BLOCK_MGR_COUNTERS, BLOCK_MGR_PROFILE_PREFIX, counters);
    tree.collectCountersWithName(BUFFER_POOL_COUNTER, BUFFER_POOL_PROFILE_PREFIX, counters);
    Long total = ImpalaRuntimeProfileUtils.sumLongCounters(counters);
    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
    if (total != null) {
      b.put(MEMORY_SPILLED, String.valueOf(total));
    }
    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    return ImmutableList.of(AttributeMetadata
        .newBuilder()
        .setName(MEMORY_SPILLED)
        .setDisplayNameKey("impala.analysis.memory_spilled.name")
        .setDescriptionKey("impala.analysis.memory_spilled.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build());
  }
}
