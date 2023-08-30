/*
 * Copyright 2019 Spotify AB.
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

package org.apache.beam.sdk.extensions.smb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.beam.sdk.extensions.smb.benchmark.CoGroupByKeyBenchmark;
import org.apache.beam.sdk.extensions.smb.benchmark.SinkBenchmark;
import org.apache.beam.sdk.extensions.smb.benchmark.SourceBenchmark;

/** Integration test intended for local execution with DirectRunner. */
public class SmbIT {

  public static void main(String[] args) throws IOException {
    final Path temp = Files.createTempDirectory("smb-");

    final Path avroSource = temp.resolve("avro");
    final Path jsonSource = temp.resolve("json");
    final Path tempLocation = temp.resolve("temp");

    final String[] smbSinkArgs = new String[args.length + 3];
    smbSinkArgs[0] = "--avroDestination=" + avroSource;
    smbSinkArgs[1] = "--jsonDestination=" + jsonSource;
    smbSinkArgs[2] = "--tempLocation=" + tempLocation;

    System.arraycopy(args, 0, smbSinkArgs, 3, args.length);

    final String[] smbSourceArgs = new String[] {
        "--avroSource=" + avroSource,
        "--jsonSource=" + jsonSource,
        "--tempLocation=" + tempLocation
    };

    final String[] coGroupByKeyArgs = new String[] {
        "--avroSource=" + avroSource.resolve("bucket-*.avro"),
        "--jsonSource=" + jsonSource.resolve("bucket-*.json"),
        "--tempLocation=" + tempLocation
    };

    try {
      SinkBenchmark.main(smbSinkArgs);
      SourceBenchmark.main(smbSourceArgs);

      // Baseline comparison with default CGBK implementation
      CoGroupByKeyBenchmark.main(coGroupByKeyArgs);
    } finally {
      Files.walk(temp)
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
    }
  }
}
