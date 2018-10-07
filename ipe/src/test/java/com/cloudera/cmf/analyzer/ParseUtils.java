package com.cloudera.cmf.analyzer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

public class ParseUtils {

  public static long US_PER_MS = 1000;
  public static long US_PER_SEC = US_PER_MS * 1000;
  public static long US_PER_MIN = US_PER_SEC * 60;
  public static long US_PER_HOUR = US_PER_MIN * 60;

  public static double parseDuration(String valueStr) {
    Pattern p = Pattern.compile("([0-9.]+)us");
    Matcher m = p.matcher(valueStr);
    if (m.matches()) {
      return Double.parseDouble(m.group(1));
    }
    p = Pattern.compile("((\\d+)h)?((\\d+)m)?((\\d+)s)?(([0-9.]+)ms)?");
    m = p.matcher(valueStr);
    if (! m.matches()) {
      throw new IllegalStateException("Duration format: " + valueStr);
    }
    double value = 0;
    if (m.group(1) != null) {
      value = Integer.parseInt(m.group(2)) * US_PER_HOUR;
    }
    if (m.group(3) != null) {
      value += Integer.parseInt(m.group(4)) * US_PER_MIN;
    }
    if (m.group(5) != null) {
      value += Integer.parseInt(m.group(6)) * US_PER_SEC;
    }
    if (m.group(7) != null) {
      value += Double.parseDouble(m.group(8)) * US_PER_MS;
    }
    return value;
  }

  public static double parseRows(String valueStr, String unitsStr) {
    double value = Double.parseDouble(valueStr);
    switch (unitsStr) {
    case "K":
      value *= 1000; // TODO: K = 1000 or 1024?
      break;
    case "M":
      value *= 1000 * 1000; // TODO: Same
      break;
    case "":
      break;
    default:
      throw new IllegalStateException("Row Suffix: " + unitsStr);
    }
    return value;
  }

  //TODO: K = 1000 or 1024?
  public static final double ONE_KB = 1024;
  public static final double ONE_MB = ONE_KB * 1024;
  public static final double ONE_GB = ONE_MB * 1024;
  public static final double ONE_TB = ONE_GB * 1024;

  public static double parseMem(String valueStr, String unitsStr) {
    double value = Double.parseDouble(valueStr);
    value = Math.max(0, value); // Some rows report negative estimates.
    switch (unitsStr) {
    case "KB":
      value *= ONE_KB;
      break;
    case "MB":
      value *= ONE_MB;
      break;
    case "GB":
      value *= ONE_GB;
      break;
    case "TB":
      value *= ONE_TB;
      break;
    case "":
    case "B":
      break;
    default:
      throw new IllegalStateException("Mem Suffix: " + unitsStr);
    }
    return value;
  }

  public static String toSecs(double us) {
    return String.format("%,.3f", us / 1000 / 1000);
  }

  public static String format(double value) {
    return String.format("%,.0f", value);
  }

  public static String[] parseExprs(String exprList) {
    List<String> exprs = new ArrayList<>();
    int posn = 0;
    for (;;) {
      int c = charAt(exprList, posn);
      while (c == ' ' || c == ',') {
        c = charAt(exprList, ++posn);
      }
      if (c == -1) { break; }
      int depth = 0;
      int inQuote = 0;;
      int start = posn;
      posn--;
      for (;;) {
        c = charAt(exprList, ++posn);
        if (c == -1) { break; }
        if (inQuote != 0) {
          if (c != inQuote) { continue; }
          inQuote = 0;
        }
        if (c == '"' || c == '`') {
          inQuote = c;
          continue;
        }
        if (c == '(' || c == '[') {
          depth++;
          continue;
        }
        if (c == ')' || c == ']') {
          depth--;
          continue;
        }
        if (depth == 0 && c == ',') {
          break;
        }
      }
      if (start == posn) { break; }
      exprs.add(exprList.substring(start, posn));
    }
    return exprs.toArray(new String[exprs.size()]);
  }

  protected static int charAt(String str, int posn) {
    if (posn >= str.length()) { return -1; }
    return str.charAt(posn);
  }

  public static double parsePlanMemory(String text) {
    Pattern p = Pattern.compile("([0-9.-]+)(\\S+)");
    Matcher m = p.matcher(text);
    Preconditions.checkState(m.matches());
    return ParseUtils.parseMem(m.group(1), m.group(2));
  }
}
