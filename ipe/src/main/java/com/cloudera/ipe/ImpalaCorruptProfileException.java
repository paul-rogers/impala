// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe;

/**
 * This exception is thrown if while processing an impala runtime profile
 * we find something wrong with the profile, like a null node name.
 */
public class ImpalaCorruptProfileException extends RuntimeException {
  private static final long serialVersionUID = 2894218026037692702L;
  
  public ImpalaCorruptProfileException(String message) {
    super(message);
  }

  public ImpalaCorruptProfileException(String message, Throwable t) {
    super(message, t);
  }
}
