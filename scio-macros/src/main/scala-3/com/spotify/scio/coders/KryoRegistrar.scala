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

package com.spotify.scio.coders

import scala.annotation.{compileTimeOnly, StaticAnnotation}
import scala.reflect.macros._

/**
 * Annotation for custom Kryo registrar classes.
 *
 * Annotated class must extend `IKryoRegistrar` and has name that ends with "KryoRegistrar".
 */
class KryoRegistrar extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = ??? // TODO: scala3 implement
}

/** Trait to be added to Kryo registrar class annotated with `@KryoRegistrar`. */
trait AnnotatedKryoRegistrar
