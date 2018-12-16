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

import java.util.List;

import org.apache.impala.analysis.ExprAnalyzer.RewriteMode;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * &&, ||, ! predicates.
 *
 */
public class CompoundPredicate extends Predicate {
  public enum Operator {
    AND("AND"),
    OR("OR"),
    NOT("NOT");

    private final String description;

    private Operator(String description) {
      this.description = description;
    }

    @Override
    public String toString() {
      return description;
    }
  }
  private final Operator op_;

  public static void initBuiltins(Db db) {
    // AND and OR are implemented as custom exprs, so they do not have a function symbol.
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.AND.name(), "",
        Lists.<Type>newArrayList(Type.BOOLEAN, Type.BOOLEAN), Type.BOOLEAN));
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.OR.name(), "",
        Lists.<Type>newArrayList(Type.BOOLEAN, Type.BOOLEAN), Type.BOOLEAN));
    db.addBuiltin(ScalarFunction.createBuiltinOperator(
        Operator.NOT.name(), "impala::CompoundPredicate::Not",
        Lists.<Type>newArrayList(Type.BOOLEAN), Type.BOOLEAN));
  }

  public CompoundPredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkArgument(op == Operator.NOT && e2 == null
        || op != Operator.NOT && e2 != null);
    if (e2 != null) children_.add(e2);
  }

  /**
   * Copy c'tor used in clone().
   */
  protected CompoundPredicate(CompoundPredicate other) {
    super(other);
    op_ = other.op_;
  }

  public Operator getOp() { return op_; }

  @Override
  public boolean localEquals(Expr that) {
    return super.localEquals(that) && ((CompoundPredicate) that).op_ == op_;
  }

  @Override
  public String debugString() {
    return Objects.toStringHelper(this)
        .add("op", op_)
        .addValue(super.debugString())
        .toString();
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    if (children_.size() == 1) {
      Preconditions.checkState(op_ == Operator.NOT);
      return "NOT " + getChild(0).toSql(options);
    } else {
      return getChild(0).toSql(options) + " " + op_.toString() + " "
          + getChild(1).toSql(options);
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.COMPOUND_PRED;
  }

  @Override
  protected void analyzeNode(Analyzer analyzer) throws AnalysisException {
    super.analyzeNode(analyzer);
    // Check that children are predicates.
    for (Expr e: children_) {
      if (!e.getType().isBoolean() && !e.getType().isNull()) {
        throw new AnalysisException(String.format("Operand '%s' part of predicate " +
            "'%s' should return type 'BOOLEAN' but returns type '%s'.",
            e.toSql(), toSql(), e.getType().toSql()));
      }
    }

    fn_ = getBuiltinFunction(analyzer, op_.toString(), collectChildReturnTypes(),
        CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    Preconditions.checkState(fn_ != null);
    Preconditions.checkState(fn_.getReturnType().isBoolean());
    castForFunctionCall(false, analyzer.isDecimalV2());
  }

  @Override
  protected void computeNodeCost() {
    super.computeNodeCost();
    if (!getChild(0).hasSelectivity() ||
        (children_.size() == 2 && !getChild(1).hasSelectivity())) {
      // Give up if one of our children has an unknown selectivity.
      selectivity_ = -1;
      return;
    }

    switch (op_) {
      case AND:
        selectivity_ = getChild(0).selectivity_ * getChild(1).selectivity_;
        break;
      case OR:
        selectivity_ = getChild(0).selectivity_ + getChild(1).selectivity_
            - getChild(0).selectivity_ * getChild(1).selectivity_;
        break;
      case NOT:
        selectivity_ = 1.0 - getChild(0).selectivity_;
        break;
    }
    selectivity_ = Math.max(0.0, Math.min(1.0, selectivity_));
  }

  @Override
  protected float computeEvalCost() {
    return hasChildCosts() ? getChildCosts() + COMPOUND_PREDICATE_COST : UNKNOWN_COST;
  }

  /**
   * Rewrites in two steps:
   * - First pass: normalize (see below). This step is aggregate-safe
   *   (does not remove aggregates).
   * - Second pass: simplify (see below). This can remove aggregates and
   *   may be rejected by the rewrite engine.
   */
  @Override
  public Expr rewrite(ExprAnalyzer exprAnalyzer) {
    if (!exprAnalyzer.isEnabled(RewriteMode.OPTIONAL)) return this;

    Expr result = normalize();
    if (result == null) {
      result = simplify();
    }
    return result == null ? this : result;
  }

  /**
   * Normalizes CompoundPredicates by ensuring that if either child of AND or OR is a
   * BoolLiteral, then the left (i.e. first) child is a BoolLiteral.
   *
   * Examples:
   * id = 0 && true -> true && id = 0
   */
  public Expr normalize() {
    if (getOp() == CompoundPredicate.Operator.NOT) return null;
    if (!(getChild(0) instanceof BoolLiteral)
        && getChild(1) instanceof BoolLiteral) {
      return new CompoundPredicate(getOp(), getChild(1), getChild(0));
    }
    return null;
  }

  /**
   * Simplifies a compound predicates with at least one BoolLiteral child, which
   * NormalizeExprsRule ensures will be the left child,  according to the following rules:
   *
   * true AND 'expr' -> 'expr'
   * false AND 'expr' -> false
   * true OR 'expr' -> true
   * false OR 'expr' -> 'expr'
   *
   * Unlike other rules here such as IF, we cannot in general simplify CompoundPredicates
   * with a NullLiteral child (unless the other child is a BoolLiteral), eg. null and
   * 'expr' is false if 'expr' is false but null if 'expr' is true.
   *
   * NOT is covered by constant folding.
   */
  public Expr simplify() {
    Expr leftChild = getChild(0);
    if (!(leftChild instanceof BoolLiteral)) return null;

    if (getOp() == CompoundPredicate.Operator.AND) {
      if (Expr.IS_TRUE_LITERAL.apply(leftChild)) {
        // TRUE AND 'expr', so return 'expr'.
        return getChild(1);
      } else {
        // FALSE AND 'expr', so return FALSE.
        return leftChild;
      }
    } else if (getOp() == CompoundPredicate.Operator.OR) {
      if (Expr.IS_TRUE_LITERAL.apply(leftChild)) {
        // TRUE OR 'expr', so return TRUE.
        return leftChild;
      } else {
        // FALSE OR 'expr', so return 'expr'.
        return getChild(1);
      }
    }
    return null;
  }

  /**
   * Negates a CompoundPredicate.
   */
  @Override
  public Expr negate() {
    if (op_ == Operator.NOT) return getChild(0);
    Expr negatedLeft = getChild(0).negate();
    Expr negatedRight = getChild(1).negate();
    Operator newOp = (op_ == Operator.OR) ? Operator.AND : Operator.OR;
    return new CompoundPredicate(newOp, negatedLeft, negatedRight);
  }

  /**
   * Creates a conjunctive predicate from a list of exprs.
   */
  public static Expr createConjunctivePredicate(List<Expr> conjuncts) {
    return createCompoundTree(conjuncts, Operator.AND);
  }

  /**
   * Creates a disjunctive predicate from a list of exprs.
   */
  public static Expr createDisjunctivePredicate(List<Expr> disjuncts) {
    return createCompoundTree(disjuncts, Operator.OR);
  }

  private static Expr createCompoundTree(List<Expr> exprs, Operator op) {
    Preconditions.checkState(op == Operator.AND || op == Operator.OR);
    Expr result = null;
    for (Expr expr: exprs) {
      if (result == null) {
        result = expr;
        continue;
      }
      result = new CompoundPredicate(op, result, expr);
    }
    return result;
  }

  @Override
  public Expr clone() { return new CompoundPredicate(this); }

  // Create an AND predicate between two exprs, 'lhs' and 'rhs'. If
  // 'rhs' is null, simply return 'lhs'.
  public static Expr createConjunction(Expr lhs, Expr rhs) {
    if (rhs == null) return lhs;
    return new CompoundPredicate(Operator.AND, rhs, lhs);
  }
}
