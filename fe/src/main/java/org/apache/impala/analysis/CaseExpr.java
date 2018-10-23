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

import java.util.HashSet;
import java.util.List;

import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TCaseExpr;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * CASE is represented using this class.
 * CASE always returns the THEN corresponding to the leftmost
 * WHEN that is TRUE, or the ELSE (or NULL if no ELSE is provided) if no WHEN is TRUE.
 * <p>
 * The internal representation of<pre><code>
 *   CASE [expr] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
 * </code></pre>
 * Each When/Then is stored as two consecutive children (whenExpr, thenExpr). If a case
 * expr is given then it is the first child. If an else expr is given then it is the
 * last child.
 * <p>
 * In prior versions, this class directly represented the <code>decode()</code>
 * function. However <code>decode()</code> is now handled as a conditional rewrite.
 *
 */
public class CaseExpr extends Expr {

  private boolean hasCaseExpr_;
  private boolean hasElseExpr_;

  public CaseExpr(Expr caseExpr, List<CaseWhenClause> whenClauses, Expr elseExpr) {
    super();
    if (caseExpr != null) {
      children_.add(caseExpr);
      hasCaseExpr_ = true;
    }
    for (CaseWhenClause whenClause: whenClauses) {
      Preconditions.checkNotNull(whenClause.getWhenExpr());
      children_.add(whenClause.getWhenExpr());
      Preconditions.checkNotNull(whenClause.getThenExpr());
      children_.add(whenClause.getThenExpr());
    }
    if (elseExpr != null) {
      children_.add(elseExpr);
      hasElseExpr_ = true;
    }
  }

  /**
   * Copy c'tor used in clone().
   */
  protected CaseExpr(CaseExpr other) {
    super(other);
    hasCaseExpr_ = other.hasCaseExpr_;
    hasElseExpr_ = other.hasElseExpr_;
  }

  public static void initBuiltins(Db db) {
    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue;
      if (t.isScalarType(PrimitiveType.CHAR)) continue;
      // TODO: case is special and the signature cannot be represented.
      // It is alternating varargs
      // e.g. case(bool, type, bool type, bool type, etc).
      // Instead we just add a version for each of the return types
      // e.g. case(BOOLEAN), case(INT), etc
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          "case", "", Lists.newArrayList(t), t));
      // Same for DECODE
      // Decode is special, and uses a custom class to handle
      // the odd argument signature.
      db.addBuiltin(new ScalarFunction.DecodeFunction(t));
    }
  }

  @Override
  public boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;
    CaseExpr expr = (CaseExpr)that;
    return hasCaseExpr_ == expr.hasCaseExpr_
        && hasElseExpr_ == expr.hasElseExpr_;
  }

  @Override
  public String toSqlImpl() {
    return toCaseSql();
  }

  @VisibleForTesting
  String toCaseSql() {
    StringBuilder output = new StringBuilder("CASE");
    int childIdx = 0;
    if (hasCaseExpr_) {
      output.append(" " + children_.get(childIdx++).toSql());
    }
    while (childIdx + 2 <= children_.size()) {
      output.append(" WHEN " + children_.get(childIdx++).toSql());
      output.append(" THEN " + children_.get(childIdx++).toSql());
    }
    if (hasElseExpr_) {
      output.append(" ELSE " + children_.get(children_.size() - 1).toSql());
    }
    output.append(" END");
    return output.toString();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.CASE_EXPR;
    msg.case_expr = new TCaseExpr(hasCaseExpr_, hasElseExpr_);
  }

  private void castCharToString(int childIndex) throws AnalysisException {
    if (children_.get(childIndex).getType().isScalarType(PrimitiveType.CHAR)) {
      children_.set(childIndex, children_.get(childIndex).castTo(ScalarType.STRING));
    }
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // Since we have no BE implementation of a CaseExpr with CHAR types,
    // we cast the CHAR-typed whenExprs and caseExprs to STRING,
    // TODO: This casting is not always correct and needs to be fixed, see IMPALA-1652.

    // Keep track of maximum compatible type of case expr and all when exprs.
    Type whenType = null;
    // Keep track of maximum compatible type of else expr and all then exprs.
    Type returnType = null;
    // Remember last of these exprs for error reporting.
    Expr lastCompatibleThenExpr = null;
    Expr lastCompatibleWhenExpr = null;
    int loopEnd = children_.size();
    if (hasElseExpr_) {
      --loopEnd;
    }
    int loopStart;
    Expr caseExpr = null;
    // Set loop start, and initialize returnType as type of castExpr.
    if (hasCaseExpr_) {
      loopStart = 1;
      castCharToString(0);
      caseExpr = children_.get(0);
      caseExpr.analyze(analyzer);
      whenType = caseExpr.getType();
      lastCompatibleWhenExpr = children_.get(0);
    } else {
      whenType = Type.BOOLEAN;
      loopStart = 0;
    }

    // Go through when/then exprs and determine compatible types.
    for (int i = loopStart; i < loopEnd; i += 2) {
      castCharToString(i);
      Expr whenExpr = children_.get(i);
      if (hasCaseExpr_) {
        // Determine maximum compatible type of the case expr,
        // and all when exprs seen so far. We will add casts to them at the very end.
        whenType = analyzer.getCompatibleType(whenType,
            lastCompatibleWhenExpr, whenExpr);
        lastCompatibleWhenExpr = whenExpr;
      } else {
        // If no case expr was given, then the when exprs should always return
        // boolean or be castable to boolean.
        if (!Type.isImplicitlyCastable(whenExpr.getType(), Type.BOOLEAN,
            false, analyzer.isDecimalV2())) {
          Preconditions.checkState(isCase());
          throw new AnalysisException("When expr '" + whenExpr.toSql() + "'" +
              " is not of type boolean and not castable to type boolean.");
        }
        // Add a cast if necessary.
        if (!whenExpr.getType().isBoolean()) castChild(Type.BOOLEAN, i);
      }
      // Determine maximum compatible type of the then exprs seen so far.
      // We will add casts to them at the very end.
      Expr thenExpr = children_.get(i + 1);
      returnType = analyzer.getCompatibleType(returnType,
          lastCompatibleThenExpr, thenExpr);
      lastCompatibleThenExpr = thenExpr;
    }
    if (hasElseExpr_) {
      Expr elseExpr = children_.get(children_.size() - 1);
      returnType = analyzer.getCompatibleType(returnType,
          lastCompatibleThenExpr, elseExpr);
    }

    // Make sure BE doesn't see TYPE_NULL by picking an arbitrary type
    if (whenType.isNull()) whenType = ScalarType.BOOLEAN;
    if (returnType.isNull()) returnType = ScalarType.BOOLEAN;

    // Add casts to case expr to compatible type.
    if (hasCaseExpr_) {
      // Cast case expr.
      if (!children_.get(0).type_.equals(whenType)) {
        castChild(whenType, 0);
      }
      // Add casts to when exprs to compatible type.
      for (int i = loopStart; i < loopEnd; i += 2) {
        if (!children_.get(i).type_.equals(whenType)) {
          castChild(whenType, i);
        }
      }
    }
    // Cast then exprs to compatible type.
    for (int i = loopStart + 1; i < children_.size(); i += 2) {
      if (!children_.get(i).type_.equals(returnType)) {
        castChild(returnType, i);
      }
    }
    // Cast else expr to compatible type.
    if (hasElseExpr_) {
      if (!children_.get(children_.size() - 1).type_.equals(returnType)) {
        castChild(returnType, children_.size() - 1);
      }
    }

    // Do the function lookup just based on the whenType.
    Type[] args = new Type[1];
    args[0] = whenType;
    fn_ = getBuiltinFunction(analyzer, "case", args,
        CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Preconditions.checkNotNull(fn_);
    type_ = returnType;
  }

  @Override
  protected float computeEvalCost() {
    if (!hasChildCosts()) return UNKNOWN_COST;
    // Compute cost as the sum of evaluating all of the WHEN exprs, plus
    // the max of the THEN/ELSE exprs.
    float maxThenCost = 0;
    float whenCosts = 0;
    for (int i = 0; i < children_.size(); ++i) {
      if (hasCaseExpr_ && i % 2 == 1) {
        // This child is a WHEN expr. BINARY_PREDICATE_COST accounts for the cost of
        // comparing the CASE expr to the WHEN expr.
        whenCosts += getChild(0).getCost() + getChild(i).getCost() +
          BINARY_PREDICATE_COST;
      } else if (!hasCaseExpr_ && i % 2 == 0) {
        // This child is a WHEN expr.
        whenCosts += getChild(i).getCost();
      } else if (i != 0) {
        // This child is a THEN or ELSE expr.
        float thenCost = getChild(i).getCost();
        if (thenCost > maxThenCost) maxThenCost = thenCost;
      }
    }
    return whenCosts + maxThenCost;
  }

  @Override
  protected void computeNumDistinctValues() {
    // Skip the first child if case expression
    int loopStart = (hasCaseExpr_ ? 1 : 0);

    // If all the outputs have a known number of distinct values (i.e. not -1), then
    // sum the number of distinct constants with the maximum NDV for the non-constants.
    //
    // Otherwise, the number of distinct values is undetermined. The input cardinality
    // (i.e. the when's) are not used.
    boolean allOutputsKnown = true;
    int numOutputConstants = 0;
    long maxOutputNonConstNdv = -1;
    HashSet<LiteralExpr> constLiteralSet = Sets.newHashSetWithExpectedSize(children_.size());

    for (int i = loopStart; i < children_.size(); ++i) {
      // The children follow this ordering:
      // [optional first child] when1 then1 when2 then2 ... else
      // After skipping optional first child, even indices are when expressions, except
      // for the last child, which can be an else expression
      if ((i - loopStart) % 2 == 0 && !(i == children_.size() - 1 && hasElseExpr_)) {
        // This is a when expression
        continue;
      }

      // This is an output expression (either then or else)
      Expr outputExpr = children_.get(i);

      if (outputExpr.isConstant()) {
        if (outputExpr.isLiteral()) {
          LiteralExpr outputLiteral = (LiteralExpr) outputExpr;
          if (constLiteralSet.add(outputLiteral)) ++numOutputConstants;
        } else {
          ++numOutputConstants;
        }
      } else {
        long outputNdv = outputExpr.getNumDistinctValues();
        if (outputNdv == -1) allOutputsKnown = false;
        maxOutputNonConstNdv = Math.max(maxOutputNonConstNdv, outputNdv);
      }
    }

    // Else unspecified => NULL constant, which is not caught above
    if (!hasElseExpr_) ++numOutputConstants;

    if (allOutputsKnown) {
      if (maxOutputNonConstNdv == -1) {
        // All must be constant, because if we hit any SlotRef, this would be set
        numDistinctValues_ = numOutputConstants;
      } else {
        numDistinctValues_ = numOutputConstants + maxOutputNonConstNdv;
      }
    } else {
      // There is no correct answer when statistics are missing. Neither the
      // known outputs nor the inputs provide information
      numDistinctValues_ = -1;
    }
  }

  private boolean isCase() { return true; }
  public boolean hasCaseExpr() { return hasCaseExpr_; }
  public boolean hasElseExpr() { return hasElseExpr_; }

  @Override
  public Expr clone() { return new CaseExpr(this); }
}
