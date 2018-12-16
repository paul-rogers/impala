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
 * Coalesces disjunctive equality predicates to an IN predicate, and merges compatible
 * equality or IN predicates into an existing IN predicate.
 * Examples:
 * (C=1) OR (C=2) OR (C=3) OR (C=4) -> C IN(1, 2, 3, 4)
 * (X+Y = 5) OR (X+Y = 6) -> X+Y IN (5, 6)
 * (A = 1) OR (A IN (2, 3)) -> A IN (1, 2, 3)
 * (B IN (1, 2)) OR (B IN (3, 4)) -> B IN (1, 2, 3, 4)
 */
public class EqualityDisjunctsToInRule implements ExprRewriteRule {

  public static ExprRewriteRule INSTANCE = new EqualityDisjunctsToInRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!Expr.IS_OR_PREDICATE.apply(expr)) return expr;

    CompoundPredicate pred = (CompoundPredicate) expr;
    Expr result = pred.rewriteInAndOtherExpr();
    if (result != null) return result;
    result = pred.rewriteEqEqPredicate();
    return result == null ? pred : result;
  }
}
