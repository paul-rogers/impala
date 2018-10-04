// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * This class extracts various pieces of information related to the DDL
 * queries.
 */
public class ImpalaDDLInformationAnalysisRule implements ImpalaAnalysisRule {

  public static final String DDL_TYPE = "ddl_type";


  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);

    String ddlType =
        tree.getSummaryMap().get(IPEConstants.IMPALA_DDL_TYPE_INFO_STRING);

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
    if (ddlType != null) {
      b.put(DDL_TYPE, ddlType);
    }
    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    AttributeMetadata ddlTypeMetadata = AttributeMetadata.newBuilder()
        .setName(DDL_TYPE)
        .setDisplayNameKey("impala.analysis.ddl_type.name")
        .setDescriptionKey("impala.analysis.ddl_type.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(ImmutableList.<String>of(
            "SHOW_TABLES",
            "SHOW_DBS",
            "USE",
            "DESCRIBE",
            "ALTER_TABLE",
            "ALTER_VIEW",
            "CREATE_DATABASE",
            "CREATE_TABLE",
            "CREATE_TABLE_AS_SELECT",
            "CREATE_TABLE_LIKE",
            "CREATE_VIEW",
            "DROP_DATABASE",
            "DROP_TABLE",
            "DROP_VIEW",
            "RESET_METADATA",
            "SHOW_FUNCTIONS",
            "CREATE_FUNCTION",
            "DROP_FUNCTION"))
        .setSupportsHistograms(true)
        .build();
    return ImmutableList.of(ddlTypeMetadata);
  }
}
