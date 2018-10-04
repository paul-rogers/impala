// Copyright (c) 2011-2012 Cloudera, Inc. All rights reserved.

package com.cloudera.ipe.util;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class JodaUtil {

  public static final DateTimeZone TZ_DEFAULT = DateTimeZone.getDefault();

  // A formatter that takes into account the default timezone. Useful for
  // turning Instants into strings.
  public static final DateTimeFormatter FORMATTER =
    DateTimeFormat.longDateTime().withZone(TZ_DEFAULT);

  // A formatter that outputs time with no space, for filename, etc.
  public static final DateTimeFormatter FORMATTER_NO_SPACE =
    DateTimeFormat.forPattern("yyyy_MM_dd-HH_mm_ss").withZone(TZ_DEFAULT);

  // A formatter that outputs time with no space and no dashed, for filename,
  // hive databases, etc.
  public static final DateTimeFormatter FORMATTER_NO_SPACE_NO_DASH =
    DateTimeFormat.forPattern("yyyy_MM_dd_HH_mm_ss").withZone(TZ_DEFAULT);

  // An ISO formatter
  public static final DateTimeFormatter FORMATTER_ISO =
      ISODateTimeFormat.dateTime();
}
