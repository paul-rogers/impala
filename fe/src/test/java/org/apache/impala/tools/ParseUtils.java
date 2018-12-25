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

package org.apache.impala.tools;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TUnit;

import com.google.common.base.Preconditions;

public class ParseUtils {

  public static long NS_PER_SEC = 1_000_000_000;
  public static long MS_PER_SEC = 1000;
  public static long US_PER_MS = 1000;
  public static long US_PER_SEC = US_PER_MS * 1000;
  public static long US_PER_MIN = US_PER_SEC * 60;
  public static long US_PER_HOUR = US_PER_MIN * 60;
  public static DateTimeFormatter START_END_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSX");

  public static class FragmentInstance {
    private final String fragmentGuid;
    private final String serverId;

    // Instance xxx:yyy (host=hhh:22000)

    public FragmentInstance(String nodeName) {
      Pattern p = Pattern.compile("Instance (\\S+) \\(host=([^(]+)\\)");
      Matcher m = p.matcher(nodeName);
      Preconditions.checkState(m.matches());
       fragmentGuid = m.group(1);
      serverId = m.group(2);
    }

    public String fragmentGuid() { return fragmentGuid; }
    public String serverId() { return serverId; }
  }

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

  public static long parseRows(String rows) {
    Pattern p = Pattern.compile("([0-9.]+)([KMB]?)");
    Matcher m = p.matcher(rows);
    if (!m.matches()) {
      throw new IllegalStateException(rows);
    }
    return parseRows(m.group(1), m.group(2));
  }

  public static long parseRows(String valueStr, String unitsStr) {
    double value = Double.parseDouble(valueStr);
    switch (unitsStr) {
    case "":
      break;
    case "K":
      value *= 1000;
      break;
    case "M":
      value *= 1_000_000;
      break;
    case "B":
      value *= 1_000_000_000;
      break;
    default:
      throw new IllegalStateException("Row Suffix: " + unitsStr);
    }
    return Math.round(value);
  }

  //TODO: K = 1000 or 1024?
  public static final double ONE_KB = 1024;
  public static final double ONE_MB = ONE_KB * 1024;
  public static final double ONE_GB = ONE_MB * 1024;
  public static final double ONE_TB = ONE_GB * 1024;

  public static long parseMem(String value) {
    Pattern p = Pattern.compile("([-0-9.]+)([A-Z]*)", Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(value);
    if (!m.matches()) {
      throw new IllegalStateException(value);
    }
    return parseMem(m.group(1), m.group(2));
  }

  public static long parseMem(String valueStr, String unitsStr) {
    double value = Double.parseDouble(valueStr);
    value = Math.max(0, value); // Some rows report negative estimates.
    switch (unitsStr.toUpperCase()) {
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
    return Math.round(value);
  }

  public static String toSecs(double us) {
    return String.format("%,.3f", us / 1000 / 1000);
  }

  public static String format(double value) {
    return String.format("%,.0f", value);
  }

  public static String format(long value) {
    return String.format("%,d", value);
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

  // Averaged Fragment F02
  // Fragment F03
  // Coordinator F04

  public static int parseFragmentId(String name) {
    Pattern p = Pattern.compile(" F(\\d+)$");
    Matcher m = p.matcher(name);
    Preconditions.checkState(m.find());
    return Integer.parseInt(m.group(1));
  }

  public static long parseStartEndTimestamp(String value) {
    return LocalDateTime.parse(value, START_END_FORMAT)
        .toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public static String formatMS(long ms) {
    return String.format("%,.3f s", (double) ms / MS_PER_SEC);
  }

  public static String formatUS(long us) {
    return String.format("%,.3f s", (double) us / US_PER_SEC);
  }

  public static String formatNS(long ns) {
    return String.format("%,.3f s", (double) ns / NS_PER_SEC);
  }

  public static double getDoubleCounter(TCounter counter) {
    if (counter.getUnit() == TUnit.DOUBLE_VALUE) {
      return Double.longBitsToDouble(counter.value);
    } else {
      return counter.value;
    }
  }

  /**
   * Convert any non-print characters to SQL escapes
   */
  public static String sanitize(String stmt) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < stmt.length(); i++) {
      char c = stmt.charAt(i);
      if (c < ' ' && c != '\n') {
        String octal = "000" + Integer.toOctalString(c);
        buf.append("\\").append(octal.substring(octal.length() - 3, octal.length()));
      } else buf.append(c);
    }
    return buf.toString();
  }

}