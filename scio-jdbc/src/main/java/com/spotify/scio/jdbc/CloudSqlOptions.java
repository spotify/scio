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

package com.spotify.scio.jdbc;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface CloudSqlOptions extends PipelineOptions {

  @Description("Cloud SQL database username")
  @Validation.Required
  String getCloudSqlUsername();

  void setCloudSqlUsername(String value);

  @Description("Cloud SQL database password")
  String getCloudSqlPassword();

  void setCloudSqlPassword(String value);

  @Description("Cloud SQL database name")
  @Validation.Required
  String getCloudSqlDb();

  void setCloudSqlDb(String value);

  @Description("Cloud SQL instance connection name, i.e project-id:zone:db-instance-name")
  @Validation.Required
  String getCloudSqlInstanceConnectionName();

  void setCloudSqlInstanceConnectionName(String value);

}
