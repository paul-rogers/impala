// Adapted from: https://blogs.oracle.com/sundararajan/programmatically-dumping-heap-from-java-applications
// See also: https://vlkan.com/blog/post/2016/08/12/hotspot-heapdump-threadump/

package org.apache.impala.catalog.local;

import javax.management.MBeanServer;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import com.sun.management.HotSpotDiagnosticMXBean;

@SuppressWarnings("restriction")
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
