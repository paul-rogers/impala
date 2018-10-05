package com.cloudera.ipe.rules;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.util.JodaUtil;
import com.google.common.collect.ImmutableList;

// From ImpalaResourceReservationAnalysisRule.WorkItemUtils

public class Builder {

  public static final ImmutableList<String>
  DEFAULT_IMPALA_RUNTIME_PROFILE_TIME_FORMATS =
      ImmutableList.of("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS");
  public static final ImmutableList<String> DEFAULT_IMPALA_SESSION_TYPES =
      ImmutableList.of(
          com.cloudera.ipe.Constants.IMPALA_SESSION_TYPE_BEESWAX,
          com.cloudera.ipe.Constants.IMPALA_SESSION_TYPE_HIVESERVER2);

  private ImmutableList<String> timeFormats = DEFAULT_IMPALA_RUNTIME_PROFILE_TIME_FORMATS;
  private ImmutableList<String> sessionTypes = DEFAULT_IMPALA_SESSION_TYPES;

  public Builder() { }

  public Builder setTimeFormatStrings(ImmutableList<String> timeFormats) {
    this.timeFormats = timeFormats;
    return this;
  }

  public Builder setSessionTypes(ImmutableList<String> sessionTypes) {
    this.sessionTypes = sessionTypes;
    return this;
  }

  public ImmutableList<String> getTimeFormatStrings() {
    return timeFormats;
  }

  public ImmutableList<String> getSessionTypes() {
    return sessionTypes;
  }

  public ImmutableList<DateTimeFormatter> getTimeFormatters() {
    ImmutableList.Builder<DateTimeFormatter> timeFormatBuilder =
        ImmutableList.builder();
    for (String format : getTimeFormatStrings()) {
      try {
        timeFormatBuilder.add(DateTimeFormat.forPattern(format)
            .withZone(JodaUtil.TZ_DEFAULT));
      } catch (IllegalArgumentException e) {
        throw new UnsupportedOperationException("Invalid time format: " + format);
      }
    }
    return timeFormatBuilder.build();
  }

  /***
   * Creates instances of IMPALA analysis rules.
   */
  public <T> ImmutableList<AnalysisRule<ImpalaRuntimeProfileTree>> build() {
    ImmutableList.Builder<AnalysisRule<ImpalaRuntimeProfileTree>> builder = new ImmutableList.Builder<AnalysisRule<ImpalaRuntimeProfileTree>>();
    builder.add(new ImpalaHDFSIOAnalysisRule());
    builder.add(new ImpalaDataStreamerAnalysisRule());
    builder.add(new ImpalaFileFormatAnalysisRule());
    builder.add(new ImpalaQueryStatusAnalysisRule());
    builder.add(new ImpalaAdmissionResultAnalysisRule());
    builder.add(new ImpalaQueryTimelineAnalysisRule());
    builder.add(new ImpalaMemorySpilledAnalysisRule());
    builder.add(new ImpalaHBaseIOAnalysisRule());
    builder.add(
        new ImpalaSessionDetailsAnalysisRule(
            getSessionTypes()));
    builder.add(
        new ImpalaMemoryUsageAnalysisRule(
            getTimeFormatters()));
    builder.add(new ImpalaQueryNumberOfFragmentsAnalysisRule());
    builder.add(new ImpalaDDLInformationAnalysisRule());
    builder.add(
        new ImpalaThreadTimeAnalysisRule(
            getTimeFormatters()));
    builder.add(
        new ImpalaResourceReservationAnalysisRule(
            getTimeFormatters()));
    builder.add(new ImpalaInsertAnalysisRule());
    builder.add(new ImpalaQueryPlanAnalysisRule());
    builder.add(new ImpalaResourceMetricsConverterAnalysisRule());
    return builder.build();
  }
}
