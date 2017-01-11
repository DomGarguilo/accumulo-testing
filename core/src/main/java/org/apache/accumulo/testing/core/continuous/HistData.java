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
package org.apache.accumulo.testing.core.continuous;

import java.io.Serializable;
import java.util.Objects;

class HistData<T> implements Comparable<HistData<T>>, Serializable {
  private static final long serialVersionUID = 1L;

  T bin;
  long count;

  HistData(T bin) {
    this.bin = bin;
    count = 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(bin) + Objects.hashCode(count);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    return obj == this || (obj != null && obj instanceof HistData && 0 == compareTo((HistData<T>) obj));
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(HistData<T> o) {
    return ((Comparable<T>) bin).compareTo(o.bin);
  }
}
