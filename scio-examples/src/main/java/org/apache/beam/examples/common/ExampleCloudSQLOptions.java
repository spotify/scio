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

package org.apache.beam.examples.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ExampleCloudSQLOptions extends PipelineOptions {

  @Description("Cloud SQL database username")
  @Validation.Required
  String getCloudSqlUser();

  void setCloudSqlUser(String value);

  @Description("Cloud SQL database password")
  @Validation.Required
  String getCloudSqlPassword();

  void setCloudSqlPassword(String value);

  @Description("Cloud SQL database name")
  @Default.String("my_database")
  String getCloudSqlDb();

  void setCloudSqlDb(String value);

  @Description("Cloud SQL instance connection name")
  @Default.String("my-project-Id:zonex:db-instance-name")
  String getCloudSqlInstanceConnectionName();

  void setCloudSqlInstanceConnectionName(String value);

}
