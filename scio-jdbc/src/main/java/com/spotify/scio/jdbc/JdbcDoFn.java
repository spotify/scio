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

package com.spotify.scio.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A {@link DoFn} that performs synchronous lookup using JDBC connection.
 *
 * @param <A> input element type.
 * @param <B> JDBC lookup value type.
 */
public abstract class JdbcDoFn<A, B> extends DoFn<A, B> {

  private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
  private transient DataSource dataSource;
  private transient Connection connection;

  protected JdbcDoFn(SerializableFunction<Void, DataSource> dataSourceProviderFn) {
    this.dataSourceProviderFn = dataSourceProviderFn;
  }

  @Setup
  public void setup() {
    dataSource = dataSourceProviderFn.apply(null);
  }

  private void createConnection() throws SQLException {
    if (dataSource == null) {
      throw new RuntimeException("DataSourceProvider " + dataSourceProviderFn + " returned null");
    }

    connection = dataSource.getConnection();
  }

  @Teardown
  public void closeConnection() throws SQLException {
    if (connection != null) {
      try {
        connection.close();
      } finally {
        connection = null;
      }
    }
  }

  @StartBundle
  public void startBundle() throws SQLException {
    // recreate a connection if it is lost
    if (connection == null || connection.isClosed()) {
      createConnection();
    }
  }

  @ProcessElement
  public void processElement(@Element A input, OutputReceiver<B> out) {
    B result = lookup(connection, input);
    out.output(result);
  }

  public abstract B lookup(Connection connection, A input);
}
