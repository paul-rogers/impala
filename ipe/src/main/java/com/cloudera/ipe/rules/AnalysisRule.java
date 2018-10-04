// Copyright (c) 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.rules;

import java.util.List;
import java.util.Map;

/**
 * Analysis rules are run on a detailed version of each work item at ingest time.
 * The rules define a work item's "synthetic attributes" which include properties
 * such as hdfs bytes read and file formats used for an Impala query work item.
 * 
 * Each rule implements a process method that takes in a detailed object
 * and returns a key-value string map (the values are not necessarily strings
 * they can be booleans or doubles). These key-value pairs can be used in filter 
 * expressions. For example if the metadata attributeName was"mySpecialRule" and
 * the type was NUMBER then a filter query might look like this: mySpecialRule > 5
 * 
 * One rule object is created for each rule so the implementations should
 * be thread-safe and state-less.
 * 
 * We may change this interface in the future so that we can publish it and
 * let customers implement this own analysis rules.
 */
public interface AnalysisRule<T> {

  /**
   * Process a details object. Process calls return a map of string from the filter
   * key to the filter value.
   */
  public Map<String, String> process(T tree);

  /**
   * Returns all the metadata associated with this rule. See the comments
   * in AvroFilterMetadata avro definition for more information on the
   * fields. Each metadata object is associated with the corresponding element
   * returned from the process call.
   */
  public List<AttributeMetadata> getFilterMetadata();
}
