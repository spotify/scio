/*
 * Copyright 2018 Spotify AB.
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
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Description("Internal options for Scio")
public interface ScioOptions extends PipelineOptions, KryoOptions {
  @Description("Scio version")
  String getScioVersion();
  void setScioVersion(String version);

  @Description("Scala version")
  String getScalaVersion();
  void setScalaVersion(String version);

  @Description("Filename to save metrics to.")
  String getMetricsLocation();
  void setMetricsLocation(String version);

  @Description("Set to true to block on ScioContext#close()")
  boolean isBlocking();
  void setBlocking(boolean value);

  @Description("Time period in scala.concurrent.duration.Duration style to block for job completion")
  String getBlockFor();
  void setBlockFor(String value);

  @Description("Custom application arguments")
  String getAppArguments();
  void setAppArguments(String arguments);

  @JsonIgnore
  @Description("Path to newline separated file with command line options")
  String getOptionsFile();
  void setOptionsFile(String optionsFile);
}
