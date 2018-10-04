// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.cmf;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.ipe.util.HistogramHelper;
import com.cloudera.ipe.util.HistogramHelper.BinScale;
import com.cloudera.ipe.util.HistogramHelper.CutPointsInfo;
import com.google.common.collect.Lists;

public class TestHistogramHelper {

  private static final String EXPRESSION = "expr";

  @Test
  public void testCutpointsForConstantDistribution() {
    // Take the points 1..100 and see what distribution it creates
    List<Double> values = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      values.add(new Double(i));
    }

    // This doesn't generate quite the ideal cut points, but it's close
    // enough. (ideally we would have 10 and the last one would be 90.0)
    CutPointsInfo cutPointInfo =
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(11, cutPoints.size());
    Assert.assertEquals(0.0, cutPoints.get(0).doubleValue(), 0.001);
    Assert.assertEquals(100.0, cutPoints.get(10).doubleValue(), 0.001);
    Assert.assertEquals(BinScale.LINEAR, cutPointInfo.getBinScale());
    Assert.assertEquals(10.0, cutPointInfo.getScaleValue().doubleValue(), 0.001);
  }

  @Test
  public void testCutpointsForConstantDistributionLessThanOne() {
    // Take the points 0.001 to 0.100 and see what distribution it creates
    List<Double> values = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      values.add(new Double(i) / 1000);
    }

    // This doesn't generate quite the ideal cut points, but it's close
    // enough. (ideally we would have 10 and the last one would be 90.0)
    CutPointsInfo cutPointInfo =
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(11, cutPoints.size());
    Assert.assertEquals(0.000, cutPoints.get(0).doubleValue(), 0.001);
    Assert.assertEquals(0.100, cutPoints.get(10).doubleValue(), 0.001);
    Assert.assertEquals(BinScale.LINEAR, cutPointInfo.getBinScale());
    Assert.assertEquals(0.01, cutPointInfo.getScaleValue().doubleValue(), 0.001);
  }

  @Test
  public void testZeroes() {
    // Create a set of all zeroes
    List<Double> values = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      values.add(new Double(0));
    }

    CutPointsInfo cutPointInfo =
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(0, cutPoints.size());
    Assert.assertEquals(BinScale.LINEAR, cutPointInfo.getBinScale());
    Assert.assertEquals(1.0, cutPointInfo.getScaleValue().doubleValue(), 0.001);
  }

  @Test
  public void testCutpointsForExpontentialDistribution() {
    List<Double> values = Lists.newArrayList();
    for (long i = 0; i < 40; i++) {
      values.add(new Double(Math.pow(2, i)));
    }

    CutPointsInfo cutPointInfo =
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(12, cutPoints.size());
    Assert.assertEquals(4.0, cutPoints.get(0).doubleValue(), 0.001);
    Assert.assertEquals(40.0, cutPoints.get(1).doubleValue(), 0.001);
    Assert.assertEquals(4.0E11, cutPoints.get(11).doubleValue(), 0.001);
    Assert.assertEquals(BinScale.EXPONENTIAL, cutPointInfo.getBinScale());
    Assert.assertEquals(10.0, cutPointInfo.getScaleValue().doubleValue(), 0.001);
  }

  @Test
  public void testRound() {
    Assert.assertEquals(400.0, HistogramHelper.round(436, false), 0.001);
    Assert.assertEquals(400.0, HistogramHelper.round(486, false), 0.001);
    Assert.assertEquals(0.05, HistogramHelper.round(0.0524, false), 0.001);
  }

  @Test
  public void testEdgeCases() {
    // No values
    List<Double> values = Lists.newArrayList();
    CutPointsInfo cutPointInfo =
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(0, cutPoints.size());

    // One value
    values = Lists.newArrayList(4.0);
    cutPointInfo = HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(0, cutPoints.size());
  }

  @Test
  public void testSkipZeroes() {
    List<Double> values = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      values.add(0D);
    }
    for (long i = 0; i < 40; i++) {
      values.add(new Double(Math.pow(2, i)));
    }

    CutPointsInfo cutPointsInfo = 
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointsInfo.getCutPoints();
    Assert.assertEquals(12, cutPoints.size());
    Assert.assertEquals(4.0, cutPoints.get(0).doubleValue(), 0.001);
    Assert.assertEquals(40.0, cutPoints.get(1).doubleValue(), 0.001);
    Assert.assertEquals(4.0E11, cutPoints.get(11).doubleValue(), 0.001);
  }

  @Test
  public void testNegativeValues() {
    List<Double> values = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      values.add(new Double(i) * -1);
    }
    CutPointsInfo cutPointInfo =
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(10, cutPoints.size());
    Assert.assertEquals(-90.0, cutPoints.get(0).doubleValue(), 0.001);
    Assert.assertEquals(0.0, cutPoints.get(9).doubleValue(), 0.001);
  }

  @Test
  public void testBuildCutPoints() {
    List<Double> cutPoints =
        HistogramHelper.buildCutPoints(BinScale.LINEAR, 10.0, 10, 1);
    Assert.assertEquals(0, cutPoints.size());

    cutPoints = HistogramHelper.buildCutPoints(BinScale.LINEAR, 10.0, 10, 4);
    Assert.assertEquals(3, cutPoints.size());
    Assert.assertEquals(20.0, cutPoints.get(0).doubleValue(), 0.001);
    Assert.assertEquals(30.0, cutPoints.get(1).doubleValue(), 0.001);
    Assert.assertEquals(40.0, cutPoints.get(2).doubleValue(), 0.001);

    cutPoints = HistogramHelper.buildCutPoints(BinScale.EXPONENTIAL, 10.0, 10, 1);
    Assert.assertEquals(0, cutPoints.size());

    cutPoints = HistogramHelper.buildCutPoints(BinScale.EXPONENTIAL, 10.0, 10, 4);
    Assert.assertEquals(3, cutPoints.size());
    Assert.assertEquals(100.0, cutPoints.get(0).doubleValue(), 0.001);
    Assert.assertEquals(1000.0, cutPoints.get(1).doubleValue(), 0.001);
    Assert.assertEquals(10000.0, cutPoints.get(2).doubleValue(), 0.001);
  }

  @Test
  public void testLargeSimilarNumbers() {
    // Take the points 100001..100100 should only return no cut-points because
    // the points are too close to distinguish with 3 sig figs.
    List<Double> values = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      values.add(new Double(100000 + i));
    }
    CutPointsInfo cutPointInfo =
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(2, cutPoints.size());
    Assert.assertEquals(100000D, cutPoints.get(0).doubleValue(), 0.001);

    // With 1001 to 1010 we should create one cut points to distinguish 1000
    // from 1010
    List<Double> values2 = Lists.newArrayList();
    for (int i = 0; i < 11; i++) {
      values2.add(new Double(1000 + i));
    }
    cutPointInfo = HistogramHelper.getSuggestedCutPoints(values2, 20, EXPRESSION);
    cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(3, cutPoints.size());
    Assert.assertEquals(1000D, cutPoints.get(0).doubleValue(), 0.001);
    Assert.assertEquals(1010D, cutPoints.get(1).doubleValue(), 0.001);
  }

  @Test
  public void testNegativeAndPositiveValues() {
    List<Double> values = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      values.add(new Double(i - 5));
    }
    CutPointsInfo cutPointInfo =
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(11, cutPoints.size());
  }

  @Test
  public void testInvalidDoubles() {
    List<Double> values = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      values.add(Double.NaN);
      values.add(Double.POSITIVE_INFINITY);
      values.add(Double.NEGATIVE_INFINITY);
    }
    CutPointsInfo cutPointInfo =
        HistogramHelper.getSuggestedCutPoints(values, 20, EXPRESSION);
    List<Double> cutPoints = cutPointInfo.getCutPoints();
    Assert.assertEquals(0, cutPoints.size());
  }
}
