/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.testing.performance.tests;

import org.apache.accumulo.core.spi.scan.ScanDispatch;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.core.spi.scan.ScanInfo;

public class TimedScanDispatcher implements ScanDispatcher {

  ScanDispatch quickExecutor1;
  ScanDispatch longExecutor1;

  long quickTime;

  // String quickExecutor;
  // String longExectuor;

  @Override
  public void init(InitParameters params) {
    // quickExecutor = params.getOptions().get("quick.executor");
    // longExectuor = params.getOptions().get("long.executor");

    quickExecutor1 = ScanDispatch.builder()
        .setExecutorName(params.getOptions().get("quick.executor")).build();
    longExecutor1 = ScanDispatch.builder()
        .setExecutorName(params.getOptions().get("quick.executor")).build();

    quickTime = Long.parseLong(params.getOptions().get("quick.time.ms"));
  }

  @Override
  public ScanDispatch dispatch(DispatchParameters params) {
    ScanInfo scanInfo = params.getScanInfo();

    if (scanInfo.getRunTimeStats().sum() < quickTime)
      return quickExecutor1;

    return longExecutor1;
  }
}
