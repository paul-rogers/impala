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
import org.apache.impala.analysis.BetweenPredicate;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.Predicate;

/**
 * Rewrites BetweenPredicates into an equivalent conjunctive/disjunctive
 * CompoundPredicate.
 * <p>
 * It can be applied to pre-analysis expr trees and therefore does not reanalyze
 * the transformation output itself.
 * Examples:
 * <ul>
 * <li><code>A BETWEEN X AND Y</code> &rarr; <code>A >= X AND A <= Y</code></li>
 * <li><code>A NOT BETWEEN X AND Y</code> &rarr; <code>A < X OR A > Y</code></li>
 * </li>
 */
public class BetweenToCompoundRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new BetweenToCompoundRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    if (!(expr instanceof BetweenPredicate)) return expr;
    BetweenPredicate bp = (BetweenPredicate) expr;
    if (bp.isNotBetween()) {
      // Rewrite into disjunction.
      Predicate lower = new BinaryPredicate(BinaryPredicate.Operator.LT,
          bp.getChild(0), bp.getChild(1));
      Predicate upper = new BinaryPredicate(BinaryPredicate.Operator.GT,
          bp.getChild(0), bp.getChild(2));
      return new CompoundPredicate(CompoundPredicate.Operator.OR, lower, upper);
    } else {
      // Rewrite into conjunction.
      Predicate lower = new BinaryPredicate(BinaryPredicate.Operator.GE,
          bp.getChild(0), bp.getChild(1));
      Predicate upper = new BinaryPredicate(BinaryPredicate.Operator.LE,
          bp.getChild(0), bp.getChild(2));
      return new CompoundPredicate(CompoundPredicate.Operator.AND, lower, upper);
    }
  }

  private BetweenToCompoundRule() {}
}
