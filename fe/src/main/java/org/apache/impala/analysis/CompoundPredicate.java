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

import java.util.ArrayList;
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
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
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
   * Rewrites in multiple steps:
   * - First pass: normalize (see below). This step is aggregate-safe
   *   (does not remove aggregates).
   * - Second pass: simplify (see below). This can remove aggregates and
   *   may be rejected by the rewrite engine.
   * - Extracts common conjuncts
   * - Coalesces disjunctive equality predicates to an IN predicate, and merges
   *   compatible equality or IN predicates into an existing IN predicate.
   *   Examples:
   *   (C=1) OR (C=2) OR (C=3) OR (C=4) -> C IN(1, 2, 3, 4)
   *   (X+Y = 5) OR (X+Y = 6) -> X+Y IN (5, 6)
   *   (A = 1) OR (A IN (2, 3)) -> A IN (1, 2, 3)
   *   (B IN (1, 2)) OR (B IN (3, 4)) -> B IN (1, 2, 3, 4)
   */
  @Override
  public Expr rewrite(ExprAnalyzer exprAnalyzer) {
    if (!exprAnalyzer.isEnabled(RewriteMode.OPTIONAL)) return this;

    Expr result = normalize();
    if (result != null) return result;
    result = simplify();
    if (result != null) return result;
    result = extractCommonConjuncts();
    if (result != null) return result;
    // Convert equality disjuncts to IN
    result = rewriteInAndOtherExpr();
    if (result != null) return result;
    result = rewriteEqEqPredicate();
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

  // Arbitrary limit the number of Expr.equals() comparisons in the O(N^2) loop below.
  // Used to avoid pathologically expensive invocations of this rule.
  // TODO: Implement Expr.hashCode() and move to a hash-based solution for the core
  // Expr.equals() comparison loop below.
  private static final int MAX_EQUALS_COMPARISONS = 30 * 30;

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
  public Expr extractCommonConjuncts() {
    if (getOp() != CompoundPredicate.Operator.OR) return null;

    // Get children's conjuncts and check
    List<Expr> child0Conjuncts = getChild(0).getConjuncts();
    List<Expr> child1Conjuncts = getChild(1).getConjuncts();
    Preconditions.checkState(!child0Conjuncts.isEmpty() && !child1Conjuncts.isEmpty());
    // Impose cost bound.
    if (child0Conjuncts.size() * child1Conjuncts.size() > MAX_EQUALS_COMPARISONS) {
      return null;
    }

    // Find common conjuncts.
    List<Expr> commonConjuncts = new ArrayList<>();
    for (Expr conjunct: child0Conjuncts) {
      if (child1Conjuncts.contains(conjunct)) {
        // The conjunct may have parenthesis but there's no need to preserve them.
        // Removing them makes the toSql() easier to read.
        conjunct.setPrintSqlInParens(false);
        commonConjuncts.add(conjunct);
      }
    }
    if (commonConjuncts.isEmpty()) return null;

    // Remove common conjuncts.
    child0Conjuncts.removeAll(commonConjuncts);
    child1Conjuncts.removeAll(commonConjuncts);

    // Check special case where one child contains all conjuncts of the other.
    // (a AND b) OR (a AND b) ==> a AND b
    // (a AND b AND c) OR (c) ==> c
    if (child0Conjuncts.isEmpty() || child1Conjuncts.isEmpty()) {
      Preconditions.checkState(!commonConjuncts.isEmpty());
      Expr result = CompoundPredicate.createConjunctivePredicate(commonConjuncts);
      return result;
    }

    // Re-assemble disjunctive predicate.
    Expr child0Disjunct = CompoundPredicate.createConjunctivePredicate(child0Conjuncts);
    child0Disjunct.setPrintSqlInParens(getChild(0).getPrintSqlInParens());
    Expr child1Disjunct = CompoundPredicate.createConjunctivePredicate(child1Conjuncts);
    child1Disjunct.setPrintSqlInParens(getChild(1).getPrintSqlInParens());
    List<Expr> newDisjuncts = Lists.newArrayList(child0Disjunct, child1Disjunct);
    Expr newDisjunction = CompoundPredicate.createDisjunctivePredicate(newDisjuncts);
    newDisjunction.setPrintSqlInParens(true);
    Expr result = CompoundPredicate.createConjunction(newDisjunction,
        CompoundPredicate.createConjunctivePredicate(commonConjuncts));
    return result;
  }

  @Override
  public List<Expr> getConjuncts() {
    if (getOp() != CompoundPredicate.Operator.AND) return super.getConjuncts();
    // TODO: we have to convert CompoundPredicate.AND to two expr trees for
    // conjuncts because NULLs are handled differently for CompoundPredicate.AND
    // and conjunct evaluation.  This is not optimal for jitted exprs because it
    // will result in two functions instead of one. Create a new CompoundPredicate
    // Operator (i.e. CONJUNCT_AND) with the right NULL semantics and use that
    // instead
    List<Expr> list = Lists.newArrayList();
    list.addAll(getChild(0).getConjuncts());
    list.addAll(getChild(1).getConjuncts());
    return list;
  }

  /**
   * Takes the children of an OR predicate and attempts to combine them into a single IN
   * predicate. The transformation is applied if one of the children is an IN predicate
   * and the other child is a compatible IN predicate or equality predicate. Returns the
   * transformed expr or null if no transformation was possible.
   */
  public Expr rewriteInAndOtherExpr() {
    if (getOp() != CompoundPredicate.Operator.OR) return null;
    Expr child0 = getChild(0);
    Expr child1 = getChild(1);
    InPredicate inPred = null;
    Expr otherPred = null;
    if (child0 instanceof InPredicate) {
      inPred = (InPredicate) child0;
      otherPred = child1;
    } else if (child1 instanceof InPredicate) {
      inPred = (InPredicate) child1;
      otherPred = child0;
    }
    if (inPred == null || inPred.isNotIn() || inPred.contains(Subquery.class) ||
        !inPred.getChild(0).equals(otherPred.getChild(0))) {
      return null;
    }

    // other predicate can be OR predicate or IN predicate
    List<Expr> newInList = Lists.newArrayList(
        inPred.getChildren().subList(1, inPred.getChildren().size()));
    if (Expr.IS_EXPR_EQ_LITERAL_PREDICATE.apply(otherPred)) {
      if (newInList.size() + 1 == Expr.EXPR_CHILDREN_LIMIT) return null;
      newInList.add(otherPred.getChild(1));
    } else if (otherPred instanceof InPredicate && !((InPredicate) otherPred).isNotIn()
        && !otherPred.contains(Subquery.class)) {
      if (newInList.size() + otherPred.getChildren().size() > Expr.EXPR_CHILDREN_LIMIT) {
        return null;
      }
      newInList.addAll(
          otherPred.getChildren().subList(1, otherPred.getChildren().size()));
    } else {
      return null;
    }

    return new InPredicate(inPred.getChild(0), newInList, false);
  }

  /**
   * Takes the children of an OR predicate and attempts to combine them into a single IN predicate.
   * The transformation is applied if both children are equality predicates with a literal on the
   * right hand side.
   * Returns the transformed expr or null if no transformation was possible.
   */
  public Expr rewriteEqEqPredicate() {
    if (getOp() != CompoundPredicate.Operator.OR) return null;
    Expr child0 = getChild(0);
    Expr child1 = getChild(1);
    if (!Expr.IS_EXPR_EQ_LITERAL_PREDICATE.apply(child0)) return null;
    if (!Expr.IS_EXPR_EQ_LITERAL_PREDICATE.apply(child1)) return null;

    if (!child0.getChild(0).equals(child1.getChild(0))) return null;
    return new InPredicate(child0.getChild(0),
        Lists.newArrayList(child0.getChild(1), child1.getChild(1)), false);
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
