// Copyright (c) 2013 Cloudera, Inc. All rights reserved.
package com.cloudera.ipe.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has a helper method that calculates suggested cut points for a
 * histogram. This algorithm will likely evolve as we test it on real world
 * data.
 */
public class HistogramHelper {

  private static Logger LOG = LoggerFactory.getLogger(
      HistogramHelper.class);
  
  public static final int MAX_ELEMS_TO_SORT = 1000;
  // If the ratio between the 95th and 5th percentile values is larger than
  // this we'll use exponential rather than linear scaling.
  public static final Double MAX_LINEAR_RATIO = 1000.0;
  private static double INTEGER_EPSILON = 0.0001;
  private static double EPSILON = 0.00001;
  private static final CutPointsInfo EMPTY_HISTOGRAM =
      new CutPointsInfo(ImmutableList.<Double>of(), BinScale.LINEAR, 1D);
  
  /**
   * The scale used to determine the size of histogram bins.
   */
  public enum BinScale {
    // Linear bins mean that bin end points increase linearlly, ie (10, 20),
    // (20, 30), (30, 40)
    LINEAR,
    // Exponential means that bin end points increase exponentially, ie (10, 100),
    // (100, 1000), (1000, 10000)
    EXPONENTIAL,
    // Custom means that there is not necessarily a fixed way the bins grow.
    CUSTOM
  }
  
  /**
   * Struct-like class that contains information about the cut-points.
   */
  public static class CutPointsInfo {
    final List<Double> cutPoints;
    final BinScale binScale;
    final Double scaleValue;

    public CutPointsInfo(
        final List<Double> cutPoints,
        final BinScale binScale,
        Double scaleValue) {
      Preconditions.checkNotNull(cutPoints);
      Preconditions.checkNotNull(binScale);
      this.cutPoints = cutPoints;
      this.binScale = binScale;
      this.scaleValue = scaleValue;
    }

    public List<Double> getCutPoints() {
      return cutPoints;
    }

    public BinScale getBinScale() {
      return binScale;
    }

    /**
     * If the bin scale is linear this is the difference between bin end points,
     * for example 10 in the case (10, 20), (20, 30), ...
     * 
     * If the bin scale is exponential it represents the ratio between bin end
     * points, for example 5 in the case (10, 50), (50, 250), (250, 1250)
     * 
     * If the bins scale is custom then this is null because we don't know
     * the scale.
     * @return
     */
    public Double getScaleValue() {
      return scaleValue;
    }
  }

  /**
   * Build cut-points based on the parameters.
   * @param binScale
   * @param scaleValue
   * @param startPoint
   * @param numBins
   * @return
   */
  public static List<Double> buildCutPoints(
      final BinScale binScale,
      final double scaleValue,
      final double startPoint,
      final int numBins) {
    Preconditions.checkNotNull(binScale);
    Preconditions.checkArgument(binScale.equals(BinScale.LINEAR) ||
        binScale.equals(BinScale.EXPONENTIAL));
    Preconditions.checkArgument(scaleValue > 0);
    Preconditions.checkArgument(startPoint >= 0);
    Preconditions.checkArgument(numBins > 0);
    
    List<Double> ret = Lists.newArrayList();
    double currentPoint = startPoint;
    for (int i = 0; i < numBins - 1; i++) {
      if (binScale.equals(BinScale.LINEAR)) {
        currentPoint += scaleValue;
      } else {
        currentPoint *= scaleValue;
      }
      ret.add(currentPoint);
    }
    return ret;
  }
  
  /**
   * Get the suggested cut points. Note that for performance reasons, this
   * function will only look at the first 1K values specified.
   * 
   * Note that we can return an empty list if the list only has one value
   * @param values
   * @param maxBins
   * @param expression
   * @return
   */
  public static CutPointsInfo getSuggestedCutPoints(
      final List<Double> values,
      final int maxBins,
      final String expression) {
    Preconditions.checkNotNull(values);
    Preconditions.checkNotNull(expression);
    // At a high level we think there are two important things to consider
    // when deciding on how to generate histogram cut points:
    // 1. Whether the distribution is best represented using linear or
    // exponential bin sizes (potentially we will add more distribution types
    // as we go)
    // 2. How to round the cut points to nice, round numbers.
    //
    // The current implementation, which is by no means ideal, works roughly
    // as follows:
    // A. Trim the list to 1K elements, if necessary
    // B. Sort the list to find the 5th and 95th percentile values
    // C. Get the ratio between the two percentile values and use that to decide
    // the answer to question #1 above (ie, linear or exponential)
    // D. Once we've decided on the rough bucketing function we see what would
    // the bucket size would be if we had 9 cut points between those values.
    // We then slightly adjust the bin sizes so that we account for consideration
    // #2 that the cut points are nice round numbers.
    //
    // We track whether all the values aren't decimals. If they are all
    // integers then we choose bin sizes that are longs as well so we don't
    // get strange bins like [0, 0.8), [0.8, 1.6) or something like that.
    boolean noDecimals = true;
    // Part A
    List<Double> valuesToSort = Lists.newArrayList();
    for (int i = 0; i < MAX_ELEMS_TO_SORT && i < values.size(); i++) {
      // Skip the zeroes because they make estimating ratios more difficult
      double value = values.get(i);
      if (Double.isInfinite(value) || Double.isNaN(value)) {
        LOG.warn("Invalid histogram value: " + value +
            " for expression: " + expression);
        continue;
      }
      if (Math.abs(value) > EPSILON) {
        valuesToSort.add(value);
        // Say if it's more than 0.0001 from an integer it's a decimal.
        if (Math.abs(value % 1) > INTEGER_EPSILON) {
          noDecimals = false;
        }
      }
    }
    if (valuesToSort.size() == 0) {
      return EMPTY_HISTOGRAM;
    }
    Collections.sort(valuesToSort);
    double min = valuesToSort.get(0);
    double max = valuesToSort.get(valuesToSort.size() - 1);
    if (min == max) {
      return EMPTY_HISTOGRAM;
    }

    // Part B
    double fifthPercentile = valuesToSort.get(valuesToSort.size() / 20);
    double ninetyFifthPercentile = valuesToSort.get(valuesToSort.size() * 19 / 20);

    // Part C. Note that if the 95th percentile is positive and the 5th
    // percentile is negative we will use linear scaling.
    double ratio =  ninetyFifthPercentile / fifthPercentile;
    // Note that because of the way we round we will always have between MIN_BINS
    // and 2 * MIN_BINS - 1 bins. See the comments for linear and exponential
    if (ratio <= MAX_LINEAR_RATIO) {
      // Part D for linear
      return getLinearCutPoints(
          fifthPercentile, ninetyFifthPercentile, maxBins, noDecimals);
    } else {
      // Part D for exponential
      return getExponentialCutPoints(
          fifthPercentile, ninetyFifthPercentile, ratio, maxBins, noDecimals);
    }
  }

  /**
   * Returns a list of the cut-points using linear scaling. This returns at
   * most 2 * (MIN_BINS - 1) cut points. Linear cut points means that the
   * difference between each pair of consecutive cut-points is the same (ie
   * {600, 700, 800, 900, ...}
   * @param fifthPercentile
   * @param ninetyFifthPercentile
   * @param cutPointsShouldBeLongs
   * @return
   */
  private static CutPointsInfo getLinearCutPoints(
      final double fifthPercentile,
      final double ninetyFifthPercentile,
      int maxBins,
      final boolean cutPointsShouldBeLongs) {
    List<Double> cutPoints = Lists.newArrayList();
    double difference = ninetyFifthPercentile - fifthPercentile;
    double binSizeEstimate = difference / ((maxBins / 2) - 1);
    // We make sure linear bin sizes aren't too small. This is to protect
    // against rounding where we get a bunch of bins that are all labeled as
    // 45K. Generally having bins that are less than 1% different doesn't seem
    // very valuable anyway.
    double minBinSize = ninetyFifthPercentile / 100;
    if (binSizeEstimate < minBinSize) {
      binSizeEstimate = minBinSize;
    }

    // Since this never rounds down by more than 50% (in the 1.999 to 1 case)
    // there should never be more than 2 * (MIN_BINS - 1) cut points.
    double binSize = round(binSizeEstimate, cutPointsShouldBeLongs);
    if (binSize <= 0) {
      binSize = 1;
    }
    // Round the first bin down to a round number (ie if the 5th percentile
    // is 1646 and the binSize is 100 then the start should be 1600).
    double currentBin = (long)(fifthPercentile / binSize) * binSize;
    cutPoints.add(currentBin);
    while (currentBin <= ninetyFifthPercentile) {
      currentBin += binSize;
      cutPoints.add(currentBin);
    }
    return new CutPointsInfo(cutPoints, BinScale.LINEAR, binSize);
  }
  
  /**
   * Returns a list of the cut-points using exponential scaling. This returns at
   * most 2 * (MIN_BINS - 1) cut points. Exponential cut points means that the
   * ratio between each pair of consecutive cut-points is the same, ie,
   * {200, 400, 800, 1600, 3200, ...}. The ratio must be positive.
   * @param fifthPercentile
   * @param ninetyFifthPercentile
   * @param ratio
   * @param maxBins
   * @param cutPointsShouldBeLongs
   * @return
   */
  private static CutPointsInfo getExponentialCutPoints(
      final double fifthPercentile,
      final double ninetyFifthPercentile,
      final double ratio,
      final int maxBins,
      final boolean cutPointsShouldBeLongs) {
    Preconditions.checkArgument(ratio > 0);
    List<Double> cutPoints = Lists.newArrayList();
    // I couldn't figure out how to take the ninth root with the Java libraries
    // without having issues with overflow / rounding, so I converted the
    // numbers to logarithms to make the numbers smaller:
    // x ^ (1/9) == e ^ (1/9 * log(x))
    double expValue = Math.log(ratio) / ((maxBins / 2) - 1);
    double binMultiplierEstimate = Math.exp(expValue);
    // Since (binMultiplier ^ 2) > binMultiplierEstimate there will never be
    // more than 2 * (MIN_BINS - 1) cut-points because:
    // binMultiplier ^(2*(MIN_BINS - 1)) > binMultiplierEstimate ^ (MIN_BINS - 1)
    double binMultiplier = round(binMultiplierEstimate, cutPointsShouldBeLongs);
    if (binMultiplier <= 2.0) {
      // This should never happen because 2 ^ 9 < 1000
      LOG.warn("Bin multiplier less than 2, ", binMultiplier);
      binMultiplier = 2;
    }
    double currentBin = round(fifthPercentile, cutPointsShouldBeLongs);
    cutPoints.add(currentBin);
    while (currentBin <= ninetyFifthPercentile) {
      currentBin *= binMultiplier;
      cutPoints.add(currentBin);
    }
    return new CutPointsInfo(cutPoints, BinScale.EXPONENTIAL, binMultiplier);
  }

  /**
   * This rounds a number to one significant figure. For example 436 becomes
   * 400. It always rounds down, so 485 also becomes 400.
   * 
   * Note that the rounded value can is between 50-100% of the original value.
   * The 50% case occurs when rounding 1XXXX to 1 where XXXX is a series of 9s
   *
   * This will also round to the nearest long value if returnLongValue is true.
   * @param value
   * @param returnLongValue
   * @return
   */
  @VisibleForTesting
  public
  static double round(double value, boolean returnLongValue) {
    BigDecimal bd = new BigDecimal(value);
    bd = bd.round(new MathContext(1, RoundingMode.DOWN));
    if (returnLongValue) {
      return bd.longValue();
    }
    return bd.doubleValue();
  }
}
