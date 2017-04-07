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

package com.spotify.scio.extra.transforms;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

class ProcessUtil {

  static String join(String[] cmdArray) {
    return Joiner.on(' ').join(cmdArray);
  }

  static String[] tokenizeCommand(String command) {
    StringTokenizer st = new StringTokenizer(command);
    String[] cmdArray = new String[st.countTokens()];
    for (int i = 0; st.hasMoreTokens(); i++)
      cmdArray[i] = st.nextToken();
    return cmdArray;
  }

  static List<String[]> tokenizeCommands(List<String> command) {
    if (command == null) {
      return null;
    }
    return command.stream().map(ProcessUtil::tokenizeCommand).collect(Collectors.toList());
  }

  static String[] createEnv(Map<String, String> environment) {
    if (environment == null || environment.isEmpty()) {
      return null;
    }
    String[] envp = new String[environment.size()];
    int i = 0;
    for (Map.Entry<String, String> e: environment.entrySet()) {
      envp[i] = e.getKey() + "=" + e.getValue();
      i++;
    }
    return envp;
  }

  static String getStdOut(Process p) throws IOException {
    return getStream(p.getInputStream());
  }

  static String getStdErr(Process p) throws IOException {
    return getStream(p.getErrorStream());
  }

  static String getStream(InputStream is) throws IOException {
    return Joiner.on('\n').join(IOUtils.readLines(is, Charsets.UTF_8));
  }

}
