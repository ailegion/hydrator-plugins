/*
 * Copyright © 2016-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.batch.aggregator.function;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

/**
 * Counts the number of times a specific column has a non-null value.
 */
public class CountNulls implements AggregateFunction<Long> {
  private final String fieldName;
  private long count;

  public CountNulls(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public void beginFunction() {
    count = 0;
  }

  @Override
  public void operateOn(StructuredRecord record) {
    if (record.get(fieldName) == null) {
      count++;
    }
  }

  @Override
  public Long getAggregate() {
    return count;
  }

  @Override
  public Schema getOutputSchema() {
    return Schema.of(Schema.Type.LONG);
  }
}
