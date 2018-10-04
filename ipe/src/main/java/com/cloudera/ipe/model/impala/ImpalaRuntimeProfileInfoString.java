// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.model.impala;

import com.google.common.base.Preconditions;

/**
 * Key-value pairs emitted into Impala profiles and available through
 * <link>ImpalaRuntimeProfileNode</link>s.
 */
public class ImpalaRuntimeProfileInfoString {
  private final String key;
  private final String value;
  
  public ImpalaRuntimeProfileInfoString(final String key, final String value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    this.key = key;
    this.value = value;
  }
  
  public final String getKey() {
    return key;
  }
  
  public final String getValue() {
    return value;
  }
}
