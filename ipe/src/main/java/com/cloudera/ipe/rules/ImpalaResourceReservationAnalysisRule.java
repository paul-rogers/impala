// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.util.IPEUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;

/**
 * This analysis rule is intended to extract attributes related to Impala's
 * resource reservations, whether YARN or Impala's own admission control.
 */
public class ImpalaResourceReservationAnalysisRule implements ImpalaAnalysisRule {

  public static final String POOL = "pool";
  public static final String RESOURCES_RESERVED_WAIT_TIME =
      "resources_reserved_wait_time";
  public static final String RESOURCES_RESERVED_WAIT_TIME_PERCENTAGE =
      "resources_reserved_wait_time_percentage";

  private ImmutableList<DateTimeFormatter> profileTimeFormatters = null;

  public ImpalaResourceReservationAnalysisRule(ImmutableList<DateTimeFormatter> profileTimeFormatters) {
    this.profileTimeFormatters = profileTimeFormatters;
  }

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();

    String pool =
        tree.getSummaryMap().get(IPEConstants.IMPALA_QUERY_ATTRIBUTE_REQUEST_POOL);
    if (pool != null) {
      b.put(POOL, pool);
    }

    Duration resourcesReservedWaitTime = tree.getEventWaitTime(IPEConstants.IMPALA_PROFILE_EVENT_RESOURCES_RESERVED,
        IPEConstants.QUERY_TIMELINE_INDEX);
    if (resourcesReservedWaitTime != null) {
      b.put(RESOURCES_RESERVED_WAIT_TIME,
          String.valueOf(resourcesReservedWaitTime.getMillis()));

      Duration queryDuration = tree.getDuration(profileTimeFormatters);
      if (queryDuration != null && queryDuration.getMillis() > 0) {
        Long resourcesReservedWaitTimePercentage =
            IPEUtils.computePercentage(
                resourcesReservedWaitTime.getMillis(),
                queryDuration.getMillis());
        if (resourcesReservedWaitTimePercentage != null) {
          b.put(RESOURCES_RESERVED_WAIT_TIME_PERCENTAGE,
              String.valueOf(resourcesReservedWaitTimePercentage));
        }
      }
    }

    return b.build();
  }


  @Override
  public List<AttributeMetadata> getFilterMetadata() {
    AttributeMetadata poolMetadata = AttributeMetadata.newBuilder()
        .setName(POOL)
        .setDisplayNameKey("impala.analysis.pool.name")
        .setDescriptionKey("impala.analysis.pool.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata resourcesReservedWaitTimeMetadata = AttributeMetadata.newBuilder()
        .setName(RESOURCES_RESERVED_WAIT_TIME)
        .setDisplayNameKey("impala.analysis.resources_reserved_wait_time.name")
        .setDescriptionKey("impala.analysis.resources_reserved_wait_time.description")
        .setFilterType(AttributeDataType.MILLISECONDS)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata resourcesReservedWaitTimePercentageMetadata = AttributeMetadata.newBuilder()
        .setName(RESOURCES_RESERVED_WAIT_TIME_PERCENTAGE)
        .setDisplayNameKey("impala.analysis.resources_reserved_wait_time_percentage.name")
        .setDescriptionKey("impala.analysis.resources_reserved_wait_time_percentage.description")
        .setFilterType(AttributeDataType.NUMBER)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .setUnitHint(IPEConstants.UNITS_PERCENT)
        .build();
   return ImmutableList.of(poolMetadata,
                           resourcesReservedWaitTimeMetadata,
                           resourcesReservedWaitTimePercentageMetadata);
  }

}
