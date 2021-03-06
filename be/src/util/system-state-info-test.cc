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

#include "common/atomic.h"
#include "testutil/gtest-util.h"
#include "util/system-state-info.h"
#include "util/time.h"

#include <thread>

namespace impala {

class SystemStateInfoTest : public testing::Test {
 protected:
  SystemStateInfo info;
};

TEST_F(SystemStateInfoTest, FirstCallReturnsZero) {
  const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
  EXPECT_EQ(0, r.user + r.system + r.iowait);

  const SystemStateInfo::NetworkUsage& n = info.GetNetworkUsage();
  EXPECT_EQ(0, n.rx_rate + n.tx_rate);
}

// Smoke test to make sure that we read non-zero values from /proc/stat.
TEST_F(SystemStateInfoTest, ReadProcStat) {
  info.ReadCurrentProcStat();
  const SystemStateInfo::CpuValues& state = info.cpu_values_[info.cpu_val_idx_];
  EXPECT_GT(state[SystemStateInfo::CPU_USER], 0);
  EXPECT_GT(state[SystemStateInfo::CPU_SYSTEM], 0);
  EXPECT_GT(state[SystemStateInfo::CPU_IDLE], 0);
  EXPECT_GT(state[SystemStateInfo::CPU_IOWAIT], 0);
}

// Smoke test to make sure that we read non-zero values from /proc/net/dev.
TEST_F(SystemStateInfoTest, ReadProcNetDev) {
  info.ReadCurrentProcNetDev();
  const SystemStateInfo::NetworkValues& state = info.network_values_[info.net_val_idx_];
  EXPECT_GT(state[SystemStateInfo::NET_RX_BYTES], 0);
  EXPECT_GT(state[SystemStateInfo::NET_RX_PACKETS], 0);
  EXPECT_GT(state[SystemStateInfo::NET_TX_BYTES], 0);
  EXPECT_GT(state[SystemStateInfo::NET_TX_PACKETS], 0);
}

// Tests parsing a line similar to the first line of /proc/stat.
TEST_F(SystemStateInfoTest, ParseProcStat) {
  // Fields are: user nice system idle iowait irq softirq steal guest guest_nice
  info.ReadProcStatString("cpu  20 30 10 70 100 0 0 0 0 0");
  const SystemStateInfo::CpuValues& state = info.cpu_values_[info.cpu_val_idx_];
  EXPECT_EQ(state[SystemStateInfo::CPU_USER], 20);
  EXPECT_EQ(state[SystemStateInfo::CPU_SYSTEM], 10);
  EXPECT_EQ(state[SystemStateInfo::CPU_IDLE], 70);
  EXPECT_EQ(state[SystemStateInfo::CPU_IOWAIT], 100);

  // Test that values larger than INT_MAX parse without error.
  info.ReadProcStatString("cpu  3000000000 30 10 70 100 0 0 0 0 0");
  const SystemStateInfo::CpuValues& changed_state = info.cpu_values_[info.cpu_val_idx_];
  EXPECT_EQ(changed_state[SystemStateInfo::CPU_USER], 3000000000);
}

// Tests parsing a string similar to the contents of /proc/net/dev.
TEST_F(SystemStateInfoTest, ParseProcNetDevString) {
  // Fields are: user nice system idle iowait irq softirq steal guest guest_nice
  string dev_net = R"(Inter-|   Receive                                                |  Transmit
   face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
       lo: 12178099975318 24167151765    0    0    0     0          0         0 12178099975318 24167151765    0    0    0     0       0          0
       br-c4d9b4cafca2: 1025814     409    0    0    0     0          0         0 70616330 1638104    0    0    0     0       0          0
         eth0: 366039609986 388580561    0    2    0     0          0  62231368 95492744135 174535524    0    0    0     0       0          0)";
  info.ReadProcNetDevString(dev_net);
  const SystemStateInfo::NetworkValues& state = info.network_values_[info.net_val_idx_];
  EXPECT_EQ(state[SystemStateInfo::NET_RX_BYTES], 366040635800);
  EXPECT_EQ(state[SystemStateInfo::NET_RX_PACKETS], 388580970);
  EXPECT_EQ(state[SystemStateInfo::NET_TX_BYTES], 95563360465);
  EXPECT_EQ(state[SystemStateInfo::NET_TX_PACKETS], 176173628);
}

// Tests that computing CPU ratios doesn't overflow
TEST_F(SystemStateInfoTest, ComputeCpuRatiosIntOverflow) {
  // Load old and new values for CPU counters. These values are from a system where we
  // have seen this code overflow before.
  info.ReadProcStatString("cpu  100952877 534 18552749 6318822633 4119619 0 0 0 0 0");
  info.ReadProcStatString("cpu  100953598 534 18552882 6318826150 4119619 0 0 0 0 0");
  info.ComputeCpuRatios();
  const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
  EXPECT_EQ(r.user, 1649);
  EXPECT_EQ(r.system, 304);
  EXPECT_EQ(r.iowait, 0);
}

// Smoke test for the public interface.
TEST_F(SystemStateInfoTest, GetCpuUsageRatios) {
  AtomicBool running(true);
  // Generate some load to observe counters > 0.
  std::thread t([&running]() { while (running.Load()); });
  for (int i = 0; i < 3; ++i) {
    SleepForMs(200);
    info.CaptureSystemStateSnapshot();
    const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
    EXPECT_GT(r.user + r.system + r.iowait, 0);
  }
  running.Store(false);
  t.join();
}

// Tests the computation logic for CPU ratios.
TEST_F(SystemStateInfoTest, ComputeCpuRatios) {
  info.ReadProcStatString("cpu  20 30 10 70 100 0 0 0 0 0");
  info.ReadProcStatString("cpu  30 30 20 70 100 0 0 0 0 0");
  info.ComputeCpuRatios();
  const SystemStateInfo::CpuUsageRatios& r = info.GetCpuUsageRatios();
  EXPECT_EQ(r.user, 5000);
  EXPECT_EQ(r.system, 5000);
  EXPECT_EQ(r.iowait, 0);
}

// Tests the computation logic for network usage.
TEST_F(SystemStateInfoTest, ComputeNetworkUsage) {
  // Two sets of values in the format of /proc/net/dev
  string dev_net_1 = R"(Inter-|   Receive                                                |  Transmit
   face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
       lo: 1000 2000    0    0    0     0          0         0 3000 4000    0    0    0     0       0          0
       br-c4d9b4cafca2: 5000     409    0    0    0     0          0         0 6000 7000    0    0    0     0       0          0
         eth0: 8000 9000    0    2    0     0          0  10000 11000 12000    0    0    0     0       0          0)";
  string dev_net_2 = R"(Inter-|   Receive                                                |  Transmit
   face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
       lo: 2000 4000    0    0    0     0          0         0 6000 8000    0    0    0     0       0          0
       br-c4d9b4cafca2: 7000     609    0    0    0     0          0         0 12000 14000    0    0    0     0       0          0
         eth0: 10000 11000    0    2    0     0          0  10000 11000 12000    0    0    0     0       0          0)";

  info.ReadProcNetDevString(dev_net_1);
  info.ReadProcNetDevString(dev_net_2);
  int period_ms = 500;
  info.ComputeNetworkUsage(period_ms);
  const SystemStateInfo::NetworkUsage& n = info.GetNetworkUsage();
  EXPECT_EQ(n.rx_rate, 8000);
  EXPECT_EQ(n.tx_rate, 12000);
}

} // namespace impala

