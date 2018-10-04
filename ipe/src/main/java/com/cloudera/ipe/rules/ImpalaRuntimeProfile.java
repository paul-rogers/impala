// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import org.apache.impala.thrift.TRuntimeProfileTree;
import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.ImpalaCorruptProfileException;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.cloudera.ipe.util.ImpalaRuntimeProfileUtils;
import com.cloudera.ipe.util.JodaUtil;
import com.cloudera.ipe.util.ThriftUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class holds the a compressed impala runtime profile. From it we can
 * generate the runtime profile thrift object.
 *
 * The data is compressed using the ZLIB compression library.
 */
public class ImpalaRuntimeProfile {
  private final static Logger LOG =
      LoggerFactory.getLogger(ImpalaRuntimeProfile.class);
  private final static TProtocolFactory PROTOCOL_FACTORY =
      new TCompactProtocol.Factory();

  public static final ImmutableList<DateTimeFormatter> DEFAULT_TIME_FORMATS;
  static {
    ImmutableList.Builder<DateTimeFormatter> builder = ImmutableList.builder();
    for (String format : IPEConstants.DEFAULT_IMPALA_RUNTIME_PROFILE_TIME_FORMATS) {
      builder.add(DateTimeFormat.forPattern(format)
          .withZone(JodaUtil.TZ_DEFAULT));
    }
    DEFAULT_TIME_FORMATS = builder.build();
  }

  private final byte[] compressedData;
  // This indicates whether this is a well-enough formed profile for us to
  // want to keep it around and show it to the user.
  private final boolean isWellFormed;
  // We save all this information so that we can build up an ImpalaQuery object
  // from the serialized avro runtime profile object.
  private final String serviceName;
  private final String frontEndHostId;
  private final Instant defaultStartTime;
  // The end time may be null
  private final Instant defaultEndTime;

  // With OPSAPS-44328, we store these properties, to be able to
  // construct ImpalaQuery object later, from the profile.
  private TRuntimeProfileTree runtimeProfile;
  private String queryId;
  private DateTimeFormatter millisecondTimeFormatter;
  private List<DateTimeFormatter> dateTimeFormatters;

  /**
   * Create an Impala runtime profile object. Callers should provide a default
   * start / end time to use in the ImpalaQuery object if we can't determine
   * the real times from the profile. (see
   * https://issues.cloudera.org/browse/IMPALA-318)
   *
   * @param compressedData
   * @param serviceName
   * @param frontEndHostId
   * @param defaultStartTime
   * @param defaultEndTime
   * @param formats - the time formats we will use to try to parse the time
   * strings in the runtime profile.
   * @param millisecondTimeFormatter
   */
  public ImpalaRuntimeProfile(
      final byte[] compressedData,
      final String serviceName,
      final String frontEndHostId,
      final Instant defaultStartTime,
      final Instant defaultEndTime,
      final List<DateTimeFormatter> formats,
      final DateTimeFormatter millisecondTimeFormatter) {
    this(compressedData,
        generateThriftProfile(compressedData),
        serviceName, frontEndHostId,
        defaultStartTime, defaultEndTime,
        formats, millisecondTimeFormatter);
  }

  public ImpalaRuntimeProfile(
      final TRuntimeProfileTree runtimeProfile,
      final String serviceName,
      final String frontEndHostId,
      final Instant defaultStartTime,
      final Instant defaultEndTime,
      final List<DateTimeFormatter> formats,
      final DateTimeFormatter millisecondTimeFormatter) {
    this(null, runtimeProfile,
        serviceName, frontEndHostId,
        defaultStartTime, defaultEndTime,
        formats, millisecondTimeFormatter);
  }

  public ImpalaRuntimeProfile(
      final byte[] compressedData,
      final TRuntimeProfileTree runtimeProfile,
      final String serviceName,
      final String frontEndHostId,
      final Instant defaultStartTime,
      final Instant defaultEndTime,
      final List<DateTimeFormatter> formats,
      final DateTimeFormatter millisecondTimeFormatter) {
    Preconditions.checkNotNull(runtimeProfile);
    Preconditions.checkNotNull(defaultStartTime);
    Preconditions.checkNotNull(serviceName);
    Preconditions.checkNotNull(frontEndHostId);
    Preconditions.checkNotNull(formats);
    Preconditions.checkNotNull(millisecondTimeFormatter);
    this.compressedData = compressedData;
    this.serviceName = serviceName;
    this.frontEndHostId = frontEndHostId;
    this.defaultStartTime = defaultStartTime;
    this.defaultEndTime = defaultEndTime;
    this.millisecondTimeFormatter = millisecondTimeFormatter;
    this.dateTimeFormatters = formats;
    // Don't keep the tree in the object because it can use a lot of memory
    // and we want to garbage collect it.
    this.runtimeProfile = runtimeProfile;
    ImpalaRuntimeProfileTree tree = ImpalaRuntimeProfileUtils
        .convertThriftProfileToTree(runtimeProfile);
    this.isWellFormed = tree.isWellFormed(formats);
    this.queryId = tree.getQueryId();
  }

  /**
   * THIS SHOULD ONLY BE USED BY TEST CODE
   * @param compressedData
   * @param serviceName
   * @param frontEndHostId
   */
  @VisibleForTesting
  public ImpalaRuntimeProfile(
      final byte[] compressedData,
      final String serviceName,
      final String frontEndHostId) {
    this(compressedData, serviceName, frontEndHostId, new Instant(), new Instant(),
        DEFAULT_TIME_FORMATS, ImpalaRuntimeProfileTree.MILLISECOND_TIME_FORMATTER);
  }

  @VisibleForTesting
  public ImpalaRuntimeProfile(
      final TRuntimeProfileTree runtimeProfile,
      final String serviceName,
      final String frontEndHostId) {
    this(runtimeProfile, serviceName, frontEndHostId, new Instant(), new Instant(),
        DEFAULT_TIME_FORMATS, ImpalaRuntimeProfileTree.MILLISECOND_TIME_FORMATTER);
  }

  public String getQueryId() {
    return queryId;
  }

  public DateTimeFormatter getMillisecondTimeFormatter() {
    return millisecondTimeFormatter;
  }

  public List<DateTimeFormatter> getDateTimeFormatters() {
    return dateTimeFormatters;
  }

  public TRuntimeProfileTree getRuntimeProfile() {
    return runtimeProfile;
  }

  public byte[] getCompressedData() {
    return compressedData;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getFrontEndHostId() {
    return frontEndHostId;
  }

  public Instant getDefaultStartTime() {
    return defaultStartTime;
  }

  public Instant getDefaultEndTime() {
    return defaultEndTime;
  }

  /**
   * Internal method that uncompresses the compressed profile.
   */
  private static byte[] getDecompressedProfile(byte[] compressedData) {
    return ImpalaRuntimeProfileUtils.decompressProfile(compressedData);
  }

  /**
   * Generates a thrift profile from the compressed serialized data. Note
   * that this operation can be expensive.
   * @return
   */
  public static TRuntimeProfileTree generateThriftProfile(byte[] compressedData) {
    Preconditions.checkNotNull(compressedData != null);
    return deserializeThriftProfile(getDecompressedProfile(compressedData));
  }

  /**
   * This indicates whether this is a well-enough formed profile for us to
   * want to keep it around and show it to the user.
   * @return
   */
  public boolean isWellFormed() {
    return isWellFormed;
  }

  /**
   * Generates a thrift profile from the compressed serialized data. Note
   * that this operation can be expensive.
   * @return
   */
  public static TRuntimeProfileTree deserializeThriftProfile(
      byte[] decompressedProfile) {
    if (decompressedProfile == null) {
      return null;
    }
    try {
      TRuntimeProfileTree thriftProfile = new TRuntimeProfileTree();
      ThriftUtil.read(decompressedProfile, thriftProfile, PROTOCOL_FACTORY);
      return thriftProfile;
    } catch (IOException e) {
      LOG.error("Error generating thrift object", e);
      throw new ImpalaCorruptProfileException("Could not decode thrift object");
    }
  }
}
