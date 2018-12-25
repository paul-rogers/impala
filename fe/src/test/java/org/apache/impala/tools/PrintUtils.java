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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class PrintUtils {

  public static String repeat(String str, int count) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < count; i++) {
      buf.append(str);
    }
    return buf.toString();
  }

  public static void writeFile(File destFile, String str) throws IOException {
    try (PrintWriter out = new PrintWriter(new FileWriter(destFile))) {
      out.println(str);
    }
  }

  public static String horizBarChart(int minNeg, int maxPos, int value) {
    StringBuilder buf = new StringBuilder();
    if (value < 0) {
      buf.append(PrintUtils.repeat(" ", value - minNeg));
      buf.append(PrintUtils.repeat("=", -value));
    } else {
      buf.append(PrintUtils.repeat(" ", -minNeg));
    }
    buf.append("|");
    if (value > 0) {
      buf.append(PrintUtils.repeat("=", value));
      buf.append(PrintUtils.repeat(" ", maxPos - value));
    }
    return buf.toString();
  }

}
