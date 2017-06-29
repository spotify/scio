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

package org.apache.cassandra.hadoop.cql3;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CqlBulkRecordWriterUtil {
  /** Workaround to expose package private constructor. */
  public static CqlBulkRecordWriter newWriter(Configuration conf,
                                              String host,
                                              int port,
                                              String username,
                                              String password,
                                              String keyspace,
                                              String table,
                                              String partitioner,
                                              String tableSchema,
                                              String insertStatement)
      throws IOException {
    ConfigHelper.setOutputInitialAddress(conf, host);
    if (port >= 0) {
      ConfigHelper.setOutputRpcPort(conf, String.valueOf(port));
    }
    if (username != null && password != null) {
      ConfigHelper.setOutputKeyspaceUserNameAndPassword(conf, username, password);
    }
    ConfigHelper.setOutputKeyspace(conf, keyspace);
    ConfigHelper.setOutputColumnFamily(conf, table);
    ConfigHelper.setOutputPartitioner(conf, partitioner);
    CqlBulkOutputFormat.setTableSchema(conf, table, tableSchema);
    CqlBulkOutputFormat.setTableInsertStatement(conf, table, insertStatement);

    // workaround for Hadoop static initialization
    if (!System.getProperties().containsKey("hadoop.home.dir") &&
        !System.getenv().containsKey("HADOOP_HOME")) {
      System.setProperty("hadoop.home.dir", "/");
    }

    DatabaseDescriptor.clientInitialization();
    return new CqlBulkRecordWriter(conf);
  }
}
