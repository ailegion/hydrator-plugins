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
import io.cdap.cdap.api.data.schema.Schema.Type;
import java.util.Objects;

public class Concat implements AggregateFunction<String> {

  private final String fieldName;
  private final Schema fieldSchema;
  private String concatString;

  public Concat(String fieldName, Schema fieldSchema) {
    this.fieldName = fieldName;
    this.fieldSchema = fieldSchema;
    Type inputType =
        fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

    if (!inputType.equals(Type.STRING)) {
      throw new IllegalArgumentException(
          String.format("Field '%s' is of unsupported non-string type '%s'. ",
              fieldName, inputType));
    }
  }

  @Override
  public String getAggregate() {
    return concatString;
  }

  @Override
  public Schema getOutputSchema() {
    return fieldSchema;
  }

  @Override
  public void beginFunction() {
    concatString = "";
  }

  @Override
  public void operateOn(StructuredRecord record) {
    if (record.get(fieldName) != null) {
      concatString = String.format("%s, %s", concatString, Objects
          .requireNonNull(record.get(fieldName)).toString());
    }
  }
}
