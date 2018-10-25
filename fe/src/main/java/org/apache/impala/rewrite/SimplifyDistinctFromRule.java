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
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.IsNullPredicate;

/**
 * Simplifies DISTINCT FROM and NOT DISTINCT FROM predicates
 * where the arguments are identical expressions.
 *
 * x IS DISTINCT FROM x -> false
 * x IS NOT DISTINCT FROM x -> true
 *
 * Note that "IS NOT DISTINCT FROM" and the "<=>" are the same.
 */
public class SimplifyDistinctFromRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new SimplifyDistinctFromRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) {
    assert expr.isAnalyzed();

    if (! (expr instanceof BinaryPredicate)) { return expr; }
    BinaryPredicate pred = (BinaryPredicate) expr;
    return simplify(pred,
        pred.getOp() == BinaryPredicate.Operator.NOT_DISTINCT);
  }

  private Expr simplify(BinaryPredicate pred, boolean sameAs) {
    Expr first = pred.getChild(0);
    Expr second = pred.getChild(1);

    // If the two terms are equal, return a constant result.
    // But, don't do this if the term is an aggregate (IMPALA-5125)
    if (first.equals(second) && ! first.isAggregate()) {
      return new BoolLiteral(sameAs);
    }

    // Convert NULL IS [NOT] DISTINCT FROM foo to foo IS [NOT] NULL
    if (first.isNullLiteral()) {
      return new IsNullPredicate(second, ! sameAs);
    }

    // Convert foo IS [NOT] DISTINCT FROM NULL to foo IS [NOT] NULL
    if (second.isNullLiteral()) {
      return new IsNullPredicate(first, ! sameAs);
    }
    return pred;
  }
}
