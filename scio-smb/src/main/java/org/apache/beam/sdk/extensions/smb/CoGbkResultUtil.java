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

import com.spotify.scio.smb.annotations.PatchedFromBeam;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

@PatchedFromBeam(origin="org.apache.beam.sdk.transforms.join.CoGbkResult")
public class CoGbkResultUtil {
  public static CoGbkResult newCoGbkResult(CoGbkResultSchema schema, List<Iterable<?>> valueMap) {
    try {
      Constructor<CoGbkResult> ctor = CoGbkResult.class
          .getDeclaredConstructor(CoGbkResultSchema.class, List.class);
      ctor.setAccessible(true);
      return ctor.newInstance(schema, valueMap);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}