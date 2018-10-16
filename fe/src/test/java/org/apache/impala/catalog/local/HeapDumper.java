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

// Adapted from: https://blogs.oracle.com/sundararajan/programmatically-dumping-heap-from-java-applications
// See also: https://vlkan.com/blog/post/2016/08/12/hotspot-heapdump-threadump/

package org.apache.impala.catalog.local;

import javax.management.MBeanServer;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import com.sun.management.HotSpotDiagnosticMXBean;

public class HeapDumper {
  // This is the name of the HotSpot Diagnostic MBean
  private static final String HOTSPOT_BEAN_NAME =
       "com.sun.management:type=HotSpotDiagnostic";

  private static HeapDumper instance;
  private final HotSpotDiagnosticMXBean hotspotMBean;

  private HeapDumper() {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      hotspotMBean =
          ManagementFactory.newPlatformMXBeanProxy(server,
          HOTSPOT_BEAN_NAME, HotSpotDiagnosticMXBean.class);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to initialize the Hotspot Mbean", e);
    }
  }

  /**
   * Dump the heap to a file
   *
   * @param file the heap dump file
   * @param live true to dump
   *             only the live objects
   * @throws IOException
   */
  static void dumpHeap(File file, boolean live) throws IOException {
    instance().dump(file, live);
  }

  static void dumpHeap(File file) {
    try {
      dumpHeap(file, true);
    } catch (IOException e) {
      throw new RuntimeException("Failed to dump the heap", e);
    }
  }

  public static HeapDumper instance() {
    if (instance == null) {
      synchronized(HeapDumper.class) {
        if (instance == null) {
          instance = new HeapDumper();
        }
      }
    }
    return instance;
  }

  public synchronized void dump(File file, boolean live) throws IOException {
    hotspotMBean.dumpHeap(file.getAbsolutePath(), live);
  }
}
