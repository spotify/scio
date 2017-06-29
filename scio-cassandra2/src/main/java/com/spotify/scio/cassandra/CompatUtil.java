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

package com.spotify.scio.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Utilities to handle Datastax Java Driver compatibility issues.
 */
class CompatUtil {

  private static RandomPartitioner randomPartitioner = new RandomPartitioner();
  private static Murmur3Partitioner murmur3Partitioner = new Murmur3Partitioner();

  public static ProtocolVersion getProtocolVersion(Cluster cluster) {
    return cluster.getConfiguration().getProtocolOptions().getProtocolVersionEnum();
  }

  public static <T> ByteBuffer serialize(DataType dataType, T value,
                                         ProtocolVersion protocolVersion) {
    return dataType.serialize(value, protocolVersion);
  }

  public static BigInteger maxToken(String partitioner) {
    switch (partitioner) {
      case "org.apache.cassandra.dht.RandomPartitioner":
        return RandomPartitioner.MAXIMUM.subtract(BigInteger.ONE);
      case "org.apache.cassandra.dht.Murmur3Partitioner":
        return BigInteger.valueOf(Murmur3Partitioner.MAXIMUM);
      default:
        throw new IllegalArgumentException("Unsupported partitioner " + partitioner);
    }
  }

  public static BigInteger minToken(String partitioner) {
    switch (partitioner) {
      case "org.apache.cassandra.dht.RandomPartitioner":
        return RandomPartitioner.ZERO;
      case "org.apache.cassandra.dht.Murmur3Partitioner":
        return BigInteger.valueOf(Murmur3Partitioner.MINIMUM.token);
      default:
        throw new IllegalArgumentException("Unsupported partitioner " + partitioner);
    }
  }

  public static BigInteger getToken(String partitioner, ByteBuffer key) {
    switch (partitioner) {
      case "org.apache.cassandra.dht.RandomPartitioner":
        return randomPartitioner.getToken(key).getToken().token;
      case "org.apache.cassandra.dht.Murmur3Partitioner":
        return BigInteger.valueOf(murmur3Partitioner.getToken(key).token)
            .add(BigInteger.valueOf(Murmur3Partitioner.MINIMUM.token));
      default:
        throw new IllegalArgumentException("Unsupported partitioner " + partitioner);
    }
  }

}
