package org.apache.beam.examples.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ExampleCloudSqlOptions extends PipelineOptions {

  /**
   * Get the username as command line option
   */
  @Description("Cloud Sql database username")
  @Validation.Required
  String getUsername();

  void setUsername(String value);

  /**
   * Get the password as command line options
   */
  @Description("Cloud Sql database password")
  @Validation.Required
  String getPassword();

  void setPassword(String value);

  /**
   * Get the database name as command line argument
   */
  @Description("Cloud SQL database name")
  @Default.String("my_database")
  String getDatabase();

  void setDatabase(String value);

  /**
   * Cloud SQL instance connection name
   */
  @Description("Cloud SQL instance connection name")
  @Default.String("my-project-Id:zonex:db-instance-name")
  String getInstanceConnectionName();

  void setInstanceConnectionName(String value);

}
