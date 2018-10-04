// Copyright (c) 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.util;

import com.cloudera.ipe.IPEConstants;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class IPEUtils {

  /**
   * Compute the percentage returning null if either of the parameters is null
   * or if the denominator is zero.
   * @param numerator
   * @param denominator
   * @return
   */
  public static Long computePercentage(Long numerator, Long denominator) {
    if (numerator == null || denominator == null) {
      return null;
    }
    // It makes more sense to return no percentage value if the denominator is
    // 0 since using either 0 or 100 with screw up aggregates and searches.
    if (denominator == 0) {
      return null;
    }
    return Math.round((double) 100 * numerator / (double) denominator);
  }

  /**
   * Returns a property, if present in the properties.
   */
  public static String getProperty(String propName, Map<String, String> properties) {
    Preconditions.checkNotNull(propName);
    if (properties == null) {
      return null;
    }
    if (properties.containsKey(propName)) {
      return properties.get(propName);
    }
    return null;
  }

  /**
   * Constructs the default date time formatters.
   */
  private static ImmutableList<DateTimeFormatter> getDefaultImpalaRuntimeProfileTimeFormatters() {
    ImmutableList.Builder<DateTimeFormatter> timeFormatBuilder = ImmutableList
        .builder();
    for (String format : IPEConstants.DEFAULT_IMPALA_RUNTIME_PROFILE_TIME_FORMATS) {
      try {
        timeFormatBuilder.add(
            DateTimeFormat.forPattern(format).withZone(JodaUtil.TZ_DEFAULT));
      } catch (IllegalArgumentException e) {
        throw new UnsupportedOperationException(
            "Invalid time format: " + format);
      }
    }
    return timeFormatBuilder.build();
  }

  /**
   * Reads the time formatters from the configuration. Time formatters are used
   * to format the milliseconds portion of the timestamp.
   */
  public static ImmutableList<DateTimeFormatter> getTimeFormatsFromProperty(Map<String, String> properties) {
    String val = getProperty(IPEConstants.IMPALA_TIMEFORMAT_PROPERTY, properties);
    ImmutableList.Builder<DateTimeFormatter> timeFormatBuilder = ImmutableList
        .builder();
    if (val == null)
      return getDefaultImpalaRuntimeProfileTimeFormatters();
    String[] formats = val.split("|||");
    for (String format : formats) {
      try {
        timeFormatBuilder.add(
            DateTimeFormat.forPattern(format).withZone(JodaUtil.TZ_DEFAULT));
      } catch (IllegalArgumentException e) {
        throw new UnsupportedOperationException(
            "Invalid time format: " + format);
      }
    }
    return timeFormatBuilder.build();
  }

  /***
   * Reads the supported session types from the configuration.
   */
  public static ImmutableList<String> getImpalaSessionTypes(Map<String, String> properties) {
    String val = getProperty(IPEConstants.IMPALA_SESSIONTYPES_PROPERTY, properties);
    if (val == null)
      return IPEConstants.DEFAULT_IMPALA_SESSION_TYPES;
    String[] sessionTypes = val.split(",");
    return ImmutableList.copyOf(sessionTypes);
  }
}
