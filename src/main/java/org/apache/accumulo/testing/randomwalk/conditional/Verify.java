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
package org.apache.accumulo.testing.randomwalk.conditional;

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.ColumnSliceFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.testing.randomwalk.RandWalkEnv;
import org.apache.accumulo.testing.randomwalk.State;
import org.apache.accumulo.testing.randomwalk.Test;

/**
 *
 */
public class Verify extends Test {

  @Override
  public void visit(State state, RandWalkEnv env, Properties props) throws Exception {
    String table = state.getString("tableName");
    AccumuloClient client = env.getAccumuloClient();

    int numAccts = state.getInteger("numAccts");
    int numBanks = state.getInteger("numBanks");
    for (int i = 0; i < numBanks; i++)
      verifyBank(table, client, Utils.getBank(i), numAccts);

  }

  private void verifyBank(String table, AccumuloClient client, String row, int numAccts)
      throws Exception {
    log.debug("Verifying bank " + row);

    int count = 0;
    int sum = 0;
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;

    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {

      scanner.enableIsolation();
      scanner.setRange(new Range(row));
      IteratorSetting iterConf = new IteratorSetting(100, "cqsl", ColumnSliceFilter.class);
      ColumnSliceFilter.setSlice(iterConf, "bal", true, "bal", true);
      scanner.clearScanIterators();
      scanner.addScanIterator(iterConf);

      for (Entry<Key,Value> entry : scanner) {
        int bal = Integer.parseInt(entry.getValue().toString());
        sum += bal;
        if (bal > max)
          max = bal;
        if (bal < min)
          min = bal;
        count++;
      }

    }

    if (count > 0 && sum != numAccts * 100) {
      throw new Exception("Sum is off " + sum);
    }

    log.debug("Verified " + row + " count = " + count + " sum = " + sum + " min = " + min
        + " max = " + max);
  }

}
