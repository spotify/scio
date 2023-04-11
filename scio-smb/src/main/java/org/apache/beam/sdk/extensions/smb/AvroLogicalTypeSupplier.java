/*
 * Copyright 2023 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.avro.SpecificDataSupplier;

public class AvroLogicalTypeSupplier extends SpecificDataSupplier {
  @Override
  public GenericData get() {
    SpecificData specificData = new SpecificData();
    specificData.addLogicalTypeConversion(new TimeConversions.DateConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.TimeConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.TimestampConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
    specificData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    specificData.addLogicalTypeConversion(new Conversions.DecimalConversion());
    specificData.addLogicalTypeConversion(new Conversions.UUIDConversion());
    return specificData;
  }
}
