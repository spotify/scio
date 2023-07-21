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
  private DataSource dataSource;
  private Connection connection;

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
  public void processElement(
      @Element A input,
      OutputReceiver<B> out) {
    B result = lookup(connection, input);
    out.output(result);
  }

  public abstract B lookup(Connection connection, A input);
}
