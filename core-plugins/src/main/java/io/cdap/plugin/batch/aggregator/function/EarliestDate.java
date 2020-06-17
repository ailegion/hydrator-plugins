/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import java.time.LocalDate;
import java.util.Optional;

public class EarliestDate<T> implements AggregateFunction<T> {

  private final String fieldName;
  private final Schema fieldSchema;
  private T earliestDate;
  private LocalDate earliestDateValue;

  public EarliestDate(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
    LogicalType logicalType =
        fieldSchema.isNullable() ? fieldSchema.getNonNullable().getLogicalType()
            : fieldSchema.getLogicalType();
    if (!Optional.ofNullable(logicalType).isPresent() || !logicalType.equals(LogicalType.DATE)) {
      invalidType();
    }
  }

  private void invalidType() {
    throw new IllegalArgumentException(
        String.format("Field '%s' is of unsupported non-date type ",
            fieldName));
  }

  @Override
  public T getAggregate() {
    return earliestDate;
  }

  @Override
  public Schema getOutputSchema() {
    return fieldSchema;
  }

  @Override
  public void beginFunction() {
    earliestDate = null;
    earliestDateValue = null;
  }

  @Override
  public void operateOn(StructuredRecord record) {
    LocalDate value = record.getDate(fieldName);
    if (value != null) {
      if (earliestDate == null || value.isBefore(earliestDateValue)) {
        setEarliestDate(record);
      }
    }
  }

  public void setEarliestDate(StructuredRecord record) {
    earliestDate = record.get(fieldName);
    earliestDateValue = record.getDate(fieldName);
  }
}
