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

package org.apache.impala.analysis;

import org.apache.impala.common.AnalysisException;

/**
 * Base interface for statements and statement-like nodes such as clauses
 * Statement-like nodes are rewritten in place. They can generate before-
 * of after- rewrite SQL. They are not subject to expression rewrites.
 */
public interface StmtNode extends ParseNode {

  /**
   * Perform semantic analysis of node and all of its children.
   * Throws exception if any semantic errors were found.
   */
  void analyze(Analyzer analyzer) throws AnalysisException;
}
