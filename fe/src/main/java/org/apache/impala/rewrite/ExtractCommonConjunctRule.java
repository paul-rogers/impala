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

package org.apache.impala.rewrite;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;

/**
 * This rule extracts common conjuncts from multiple disjunctions when it is applied
 * recursively bottom-up to a tree of CompoundPredicates.
 * It can be applied to pre-analysis expr trees and therefore does not reanalyze
 * the transformation output itself.
 *
 * Examples:
 * (a AND b AND c) OR (b AND d) ==> b AND ((a AND c) OR (d))
 * (a AND b) OR (a AND b) ==> a AND b
 * (a AND b AND c) OR (c) ==> c
 */
public class ExtractCommonConjunctRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new ExtractCommonConjunctRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!Expr.IS_OR_PREDICATE.apply(expr)) return expr;
    Expr result = ((CompoundPredicate) expr).extractCommonConjuncts();
    return result == null ? expr : result;
  }

  private ExtractCommonConjunctRule() {}
}
