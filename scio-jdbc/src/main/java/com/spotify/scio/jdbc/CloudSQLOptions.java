package com.spotify.scio.jdbc;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface CloudSQLOptions extends PipelineOptions {

  @Description("Cloud SQL database username")
  @Validation.Required
  String getCloudSqlUser();

  void setCloudSqlUser(String value);

  @Description("Cloud SQL database password")
  @Validation.Required
  String getCloudSqlPassword();

  void setCloudSqlPassword(String value);

  @Description("Cloud SQL database name")
  @Validation.Required
  String getCloudSqlDb();

  void setCloudSqlDb(String value);

  @Description("Cloud SQL instance connection name. i.e my-project-Id:zonex:db-instance-name")
  @Validation.Required
  String getCloudSqlInstanceConnectionName();

  void setCloudSqlInstanceConnectionName(String value);

}
