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
package com.spotify.scio.schemas.instances

import com.spotify.scio.IsJavaBean
import com.spotify.scio.schemas.{Arr, RawRecord, Schema}
import org.apache.beam.sdk.schemas.JavaBeanSchema

import scala.reflect.ClassTag

trait JavaInstances {

  implicit def jListSchema[T](implicit s: Schema[T]): Schema[java.util.List[T]] =
    Arr(s, identity, identity)

  implicit def javaBeanSchema[T: IsJavaBean: ClassTag]: RawRecord[T] =
    RawRecord[T](new JavaBeanSchema())

}
