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

import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroDataSupplier;

public class GenericDataLogicalTypeSupplier implements AvroDataSupplier {
  @Override
  public GenericData get() {
    GenericData data = new GenericData();
    org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.addLogicalTypeConversions(data);
    return data;
  }
}
