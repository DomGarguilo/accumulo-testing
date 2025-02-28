/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.testing.randomwalk.bulk;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chooses whether an operation should be queued based on the current thread pool queue length and
 * the number of available TServers.
 */
public class SelectiveQueueing {
  private static final Logger log = LoggerFactory.getLogger(SelectiveQueueing.class);

  public static boolean shouldQueueOperation(State state, RandWalkEnv env) {
    final ThreadPoolExecutor pool = (ThreadPoolExecutor) state.get("pool");
    long queuedThreads = pool.getTaskCount() - pool.getActiveCount() - pool.getCompletedTaskCount();
    final AccumuloClient client = env.getAccumuloClient();
    int numTservers = client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).size();

    if (!shouldQueue(queuedThreads, numTservers)) {
      log.info("Not queueing because of " + queuedThreads + " outstanding tasks");
      return false;
    }

    return true;
  }

  private static boolean shouldQueue(long queuedThreads, int numTservers) {
    return queuedThreads < numTservers * 50L;
  }
}
