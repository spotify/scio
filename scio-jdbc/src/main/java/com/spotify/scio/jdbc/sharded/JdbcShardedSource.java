/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.jdbc.sharded;

import com.spotify.scio.jdbc.JdbcConnectionOptions;
import com.spotify.scio.jdbc.sharded.JdbcShardedReadOptions;
import com.spotify.scio.jdbc.sharded.Range;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcShardedSource<T> extends BoundedSource<T> {

  private static final Logger log = LoggerFactory.getLogger(JdbcShardedSource.class);

  private final Range<Long> range;
  private final Coder<T> coder;
  private final JdbcShardedReadOptions<T> readOptions;

  public JdbcShardedSource(
      JdbcShardedReadOptions<T> readOptions, Coder<T> coder, Range<Long> range) {
    this.readOptions = readOptions;
    this.range = range;
    this.coder = coder;
  }

  private static Connection createConnection(JdbcConnectionOptions connectionOptions)
      throws Exception {
    final Connection connection =
        DriverManager.getConnection(
            connectionOptions.connectionUrl(),
            connectionOptions.username(),
            connectionOptions.password().get());
    connection.setAutoCommit(false);
    log.info("Created connection to [{}]", connectionOptions.connectionUrl());

    return connection;
  }

  private List<Range<Long>> partitionRange(Range<Long> range, int partitionsCount) {
    final long partitionSize = (range.max() - range.min()) / partitionsCount;

    return Stream.iterate(0, i -> i + 1)
        .limit(partitionsCount)
        .map(
            i ->
                (i < partitionsCount - 1)
                    ? new Range<Long>(
                        range.min() + i * partitionSize,
                        range.min() + i * partitionSize + partitionSize - 1)
                    : new Range<Long>(range.min() + i * partitionSize, range.max()))
        .collect(Collectors.toList());
  }

  private Range<Long> getShardColumnRange() throws Exception {
    try (Connection connection = createConnection(readOptions.connectionOptions())) {
      String query =
          String.format(
              "SELECT min(%s) min, max(%s) max FROM %s",
              readOptions.shardColumn(), readOptions.shardColumn(), readOptions.tableName());

      log.info("Executing query = [{}]", query);

      final ResultSet rs = connection.createStatement().executeQuery(query);

      if (rs.next()) {
        return new Range<Long>(rs.getLong("min"), rs.getLong("max"));
      } else {
        log.warn("The table is empty. Nothing to read.");
        return null;
      }
    }
  }

  @Override
  public List<? extends BoundedSource<T>> split(
      final long desiredBundleSizeBytes, final PipelineOptions options) throws Exception {

    final Range<Long> shardColumnRange = getShardColumnRange();

    if (shardColumnRange == null) {
      return Collections.emptyList();
    }

    final long lowerBound = shardColumnRange.min();
    final long upperBound = shardColumnRange.max();

    final int partitionsCount =
        (upperBound - lowerBound < readOptions.fetchSize()) ? 1 : readOptions.numShards();

    log.info(
        "Going to partition the read into {} ranges for lowerBound={}, "
            + "upperBound={}, numShard={}",
        partitionsCount,
        lowerBound,
        upperBound,
        readOptions.numShards());

    final List<Range<Long>> partitions = partitionRange(shardColumnRange, partitionsCount);

    return partitions.stream()
        .map(rng -> new JdbcShardedSource<T>(readOptions, coder, rng))
        .collect(Collectors.toList());
  }

  @Override
  public long getEstimatedSizeBytes(final PipelineOptions options) throws Exception {
    return 0;
  }

  @Override
  public BoundedReader<T> createReader(final PipelineOptions options) throws IOException {
    return new JdbcShardedReader<T>(this);
  }

  @Override
  public Coder<T> getOutputCoder() {
    return coder;
  }

  public static class JdbcShardedReader<T> extends BoundedReader<T> {

    private final JdbcShardedSource<T> source;
    private Connection connection = null;
    private ResultSet resultSet = null;

    public JdbcShardedReader(JdbcShardedSource<T> source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      try {
        connection = createConnection(source.readOptions.connectionOptions());

        final Statement statement = connection.createStatement();
        if (source.readOptions.fetchSize() != JdbcShardedReadOptions.UnboundedFetchSize()) {
          log.info("Setting a user defined fetch size: [{}]", source.readOptions.fetchSize());
          statement.setFetchSize(source.readOptions.fetchSize());
        }

        final String query =
            String.format(
                "SELECT * FROM %s WHERE %s >= %d and %s <= %d",
                source.readOptions.tableName(),
                source.readOptions.shardColumn(),
                source.range.min(),
                source.readOptions.shardColumn(),
                source.range.max());

        log.info("Running a query: [{}]", query);
        resultSet = statement.executeQuery(query);

        return resultSet.next();
      } catch (Exception e) {
        throw new IOException("Failed to read the first record", e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      try {
        return resultSet.next();
      } catch (SQLException e) {
        throw new IOException("Failed to advance to the next element", e);
      }
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      try {
        return source.readOptions.rowMapper().apply(resultSet);
      } catch (Exception e) {
        throw new NoSuchElementException("Failed to read or decode the element");
      }
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public void close() throws IOException {
      if (connection != null) {
        try {
          connection.close();
          log.info("JDBC connection closed");
        } catch (SQLException e) {
          log.error("Failed to close JDBC connection", e);
        }
      }
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return source;
    }
  }
}
