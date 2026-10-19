/*
 * Copyright 2026 Spotify AB.
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

package com.spotify.scio.options;

import java.util.List;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Description("Options for Avro")
public interface AvroOptions extends PipelineOptions {
  @Description(
      "Comma-separated packages whose classes avro 1.12.2+ may load, in addition to avro"
          + " generated classes, e.g. for reflect records: com.example,org.example.pojos."
          + " Applies to Beam workers, unlike the org.apache.avro.SERIALIZABLE_PACKAGES system"
          + " property")
  List<String> getAvroTrustedPackages();

  void setAvroTrustedPackages(List<String> value);
}
