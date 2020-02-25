/*
 * Copyright 2017 Spotify AB.
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Description("Options for KryoAtomicCoder")
public interface KryoOptions extends PipelineOptions {
  @JsonIgnore
  @Description("Set buffer size")
  @Default.Integer(64 * 1024)
  int getKryoBufferSize();

  void setKryoBufferSize(int bufferSize);

  @JsonIgnore
  @Description("Set maximum buffer size")
  @Default.Integer(64 * 1024 * 1024)
  int getKryoMaxBufferSize();

  void setKryoMaxBufferSize(int bufferSize);

  @JsonIgnore
  @Description("Set to false to disable reference tracking")
  @Default.Boolean(true)
  boolean getKryoReferenceTracking();

  void setKryoReferenceTracking(boolean referenceTracking);

  @JsonIgnore
  @Description("Set to true to require registration")
  @Default.Boolean(false)
  boolean getKryoRegistrationRequired();

  void setKryoRegistrationRequired(boolean registrationRequired);
}
