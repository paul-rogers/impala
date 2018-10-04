// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import com.cloudera.ipe.IPEConstants;
import com.cloudera.ipe.AttributeDataType;
import com.cloudera.ipe.model.impala.ImpalaRuntimeProfileTree;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * This class extracts various pieces of information related to the session
 * associated with the query.
 */
public class ImpalaSessionDetailsAnalysisRule implements ImpalaAnalysisRule {

  public static final String IMPALA_VERSION = "impala_version";
  public static final String SESSION_ID = "session_id";
  public static final String SESSION_TYPE = "session_type";
  public static final String NETWORK_ADDRESS = "network_address";
  public static final String CONNECTED_USER = "connected_user";
  public static final String DELEGATED_USER = "delegated_user";
  private ImmutableList<String> impalaSessionTypes;

  public ImpalaSessionDetailsAnalysisRule(ImmutableList<String> impalaSessionTypes) {
    this.impalaSessionTypes = impalaSessionTypes;
  }

  @VisibleForTesting
  public ImmutableList<String> getSessionTypes() {
    return impalaSessionTypes;
  }

  @Override
  public Map<String, String> process(ImpalaRuntimeProfileTree tree) {
    Preconditions.checkNotNull(tree);

    String impalaVersion =
        tree.getSummaryMap().get(IPEConstants.IMPALA_IMPALA_VERSION_INFO_STRING);
    String sessionID =
        tree.getSummaryMap().get(IPEConstants.IMPALA_SESSION_ID_INFO_STRING);
    String sessionType =
        tree.getSummaryMap().get(IPEConstants.IMPALA_SESSION_TYPE_INFO_STRING);
    String networkAddress =
        tree.getSummaryMap().get(IPEConstants.IMPALA_NETWORK_ADDRESS_INFO_STRING);
    String connectedUser =
        tree.getSummaryMap().get(IPEConstants.IMPALA_CONNECTED_USER_INFO_STRING);
    String delegatedUser =
        tree.getSummaryMap().get(IPEConstants.IMPALA_DELEGATED_USER_INFO_STRING);

    ImmutableMap.Builder<String, String> b = ImmutableMap.builder();
    if (impalaVersion != null) {
      b.put(IMPALA_VERSION, impalaVersion);
    }
    if (sessionID != null) {
      b.put(SESSION_ID, sessionID);
    }
    if (sessionType != null) {
      b.put(SESSION_TYPE, sessionType);
    }
    if (networkAddress != null) {
      b.put(NETWORK_ADDRESS, networkAddress);
    }
    // We use StringUtils.isEmpty to test the following user names since empty
    // user names are not valid. In particular we know that delegated user
    // is set to empty even when delegation is not in use. We would rather
    // not have the attribute set than have an empty value.
    if (!StringUtils.isEmpty(connectedUser)) {
      b.put(CONNECTED_USER, connectedUser);
    }
    if (!StringUtils.isEmpty(delegatedUser)) {
      b.put(DELEGATED_USER, delegatedUser);
    }
    return b.build();
  }

  @Override
  public List<AttributeMetadata> getFilterMetadata() {

    AttributeMetadata impalaVersionMetadata = AttributeMetadata.newBuilder()
        .setName(IMPALA_VERSION)
        .setDisplayNameKey("impala.analysis.impala_version.name")
        .setDescriptionKey("impala.analysis.impala_version.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata sessionIdMetadata = AttributeMetadata.newBuilder()
        .setName(SESSION_ID)
        .setDisplayNameKey("impala.analysis.session_id.name")
        .setDescriptionKey("impala.analysis.session_id.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata sessionTypeMetadata = AttributeMetadata.newBuilder()
        .setName(SESSION_TYPE)
        .setDisplayNameKey("impala.analysis.session_type.name")
        .setDescriptionKey("impala.analysis.session_type.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(impalaSessionTypes)
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata networkAddressMetadata = AttributeMetadata.newBuilder()
        .setName(NETWORK_ADDRESS)
        .setDisplayNameKey("impala.analysis.network_address.name")
        .setDescriptionKey("impala.analysis.network_address.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata connectedUserMetadata = AttributeMetadata.newBuilder()
        .setName(CONNECTED_USER)
        .setDisplayNameKey("impala.analysis.connected_user.name")
        .setDescriptionKey("impala.analysis.connected_user.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    AttributeMetadata delegatedUserMetadata = AttributeMetadata.newBuilder()
        .setName(DELEGATED_USER)
        .setDisplayNameKey("impala.analysis.delegated_user.name")
        .setDescriptionKey("impala.analysis.delegated_user.description")
        .setFilterType(AttributeDataType.STRING)
        .setValidValues(ImmutableList.<String>of())
        .setSupportsHistograms(true)
        .build();
    return ImmutableList.of(
        impalaVersionMetadata,
        sessionIdMetadata,
        sessionTypeMetadata,
        networkAddressMetadata,
        connectedUserMetadata,
        delegatedUserMetadata);
  }
}
