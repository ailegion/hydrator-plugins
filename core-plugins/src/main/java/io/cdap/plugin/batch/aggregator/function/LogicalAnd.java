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
import io.cdap.cdap.api.data.schema.Schema.Type;

/**
 * Counts the number of times a specific column has a non-null value.
 */
public class LogicalAnd implements AggregateFunction<Boolean> {

  private final String fieldName;
  private boolean logicalAnd;

  public LogicalAnd(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public void beginFunction() {
    logicalAnd = true;
  }

  @Override
  public void operateOn(StructuredRecord record) {
    Object value = record.get(fieldName);
    if (value != null && !(Boolean) value) {
      logicalAnd = false;
    }
  }

  @Override
  public Boolean getAggregate() {
    return logicalAnd;
  }

  @Override
  public Schema getOutputSchema() {
    return Schema.of(Type.BOOLEAN);
  }
}
