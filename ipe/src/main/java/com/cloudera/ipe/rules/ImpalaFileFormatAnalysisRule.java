// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.IPEConstants;
import org.apache.impala.thrift.TRuntimeProfileNode;
import org.apache.impala.thrift.TRuntimeProfileTree;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * This rule returns a comma-separated, alphabetically ordered list of all
 * the file formats used in a query.
 */
public class ImpalaFileFormatAnalysisRule implements ImpalaAnalysisRule {

  public final static String FILE_FORMATS = "file_formats";


  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);
    TRuntimeProfileTree thriftTree = tree.getThriftTree();
    Set<String> fileFormats = Sets.newTreeSet();
    // Extract the file format from any nodes that have it.
    for (TRuntimeProfileNode node : thriftTree.getNodes()) {
      if (node.getInfo_strings() == null) {
        continue;
      }
      for (Entry<String, String> infoString : node.getInfo_strings().entrySet()) {
        String key = infoString.getKey();
        if (StringUtils.equals(key, IPEConstants.IMPALA_PROFILE_FILE_FORMATS)) {
          String value = infoString.getValue();
          // The value is in the format: list[Format/Compression:Count]
          // For example: TEXT/NONE:12 PARQUET/NONE:12
          for (String formatCounter : safeStringSplit(value, " ")) {
            String[] keyArray = formatCounter.split(":");
            if (keyArray.length == 2) {
              fileFormats.add(keyArray[0]);
            }
          }
        }
      }
    }
    return ImmutableMap.of(FILE_FORMATS, StringUtils.join(fileFormats, ","));
  }

  /**
   * This function splits a string based on the separator. If the string is
   * null it returns an empty array.
   */
  private String[] safeStringSplit(
      final String inputString,
      final String separator) {
    if (inputString == null) {
      return new String[0];
    } else if (separator == null) {
      return new String[0];
    }
    return inputString.split(separator);
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    return ImmutableList.of(AttributeMetadata.newBuilder()
        .setName(FILE_FORMATS)
        .setDisplayNameKey("impala.analysis.file_formats.name")
        .setDescriptionKey("impala.analysis.file_formats.description")
        .setFilterType(AttributeDataType.STRING)
         // For now I don't want to keep this updated with the list of file formats
         // Impala supports, but we might want to consider that at some point.
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(false)
        .build());
  }
}
