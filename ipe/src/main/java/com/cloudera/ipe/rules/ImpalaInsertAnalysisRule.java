// Copyright (c) 2014 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class ImpalaInsertAnalysisRule implements ImpalaAnalysisRule {

  public final static String ROWS_INSERTED = "rows_inserted";
  public final static String HDFS_BYTES_WRITTEN = "hdfs_bytes_written";

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);

    Long rowsInserted = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_ROWS_INSERTED,
        IPEConstants.IMPALA_PROFILE_ALL_NODES_PREFIX);
    Long hdfsBytesWritten = tree.getSumAllCounterValues(
        IPEConstants.IMPALA_PROFILE_BYTES_WRITTEN,
        IPEConstants.IMPALA_PROFILE_HDFS_SINK_NODE_PREFIX);

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();

    if (rowsInserted != null) {
      b.put(ROWS_INSERTED, String.valueOf(rowsInserted));
    }
    if (hdfsBytesWritten != null) {
      b.put(HDFS_BYTES_WRITTEN, String.valueOf(hdfsBytesWritten));
    }

    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    return ImmutableList.of(
        AttributeMetadata.newBuilder()
        .setName(ROWS_INSERTED)
        .setDisplayNameKey("impala.analysis.rows_inserted.name")
        .setDescriptionKey("impala.analysis.rows_inserted.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_ROWS)
        .build(),
        AttributeMetadata.newBuilder()
        .setName(HDFS_BYTES_WRITTEN)
        .setDisplayNameKey("impala.analysis.hdfs_bytes_written.name")
        .setDescriptionKey("impala.analysis.hdfs_bytes_written.description")
        .setFilterType(AttributeDataType.BYTES)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build());
  }
}
