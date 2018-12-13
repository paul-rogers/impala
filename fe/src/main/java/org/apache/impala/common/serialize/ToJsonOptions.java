// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.common.serialize;

/**
 * Options to control the level of detail provided by the JSON
 * serialized version of the AST.
 */
public class ToJsonOptions {
  private boolean showSource_;
  private boolean showOutput_;
  private boolean showInternals_;
  private boolean elide_;
  private boolean dedup_;

  /**
   * Include the source SQL.
   */
  public ToJsonOptions showSource(boolean flag)
    { showSource_ = flag; return this; }
  /**
   * Include details of the structures created by analyais, such
   * as the output result set.
   */
  public ToJsonOptions showOutput(boolean flag)
    { showOutput_ = flag; return this; }
  /**
   * Show internal structures such as substitution maps.
   */
  public ToJsonOptions showInternals(boolean flag)
    { showInternals_ = flag; return this; }
  /**
   * Create more compact JSON by "eliding" structures: omit fields which
   * are null or false, fields at their default values, etc.
   */
  public ToJsonOptions elide(boolean flag)
    { elide_ = flag; return this; }
  /**
   * Dedup the output by giving each object an ID, then including just the
   * id if the object appears again in the output. The ID is based on tree
   * structure, so it tends to be consistent across runs.
   */
  public ToJsonOptions dedup(boolean flag)
    { dedup_ = flag; return this; }

  /**
   * Turn on all options.
   */
  public static ToJsonOptions full() {
    return new ToJsonOptions()
        .showSource(true)
        .showOutput(true)
        .showInternals(true);
  }

  /**
   * Include all AST details in the JSON, but do so in a compact form.
   */
  public static ToJsonOptions fullCompact() {
    return full().elide(true).dedup(true);
  }

  public boolean showSource() { return showSource_; }
  public boolean showOutput() { return showOutput_; }
  public boolean showInternals() { return showInternals_; }
  public boolean elide() { return elide_; }
  public boolean dedup() { return dedup_;}
}
