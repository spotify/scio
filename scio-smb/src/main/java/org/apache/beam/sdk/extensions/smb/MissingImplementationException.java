/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import org.apache.commons.lang3.StringUtils;

public class MissingImplementationException extends RuntimeException {

  public MissingImplementationException(String format, Throwable cause) {
    super(buildMessage(format), cause);
  }

  private static String buildMessage(String format) {
    return String.format(
        "%s is not part of the classpath. Add the 'com.spotify:scio-%s' dependency",
        StringUtils.capitalize(format), format);
  }
}
