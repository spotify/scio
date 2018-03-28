/*
 * Copyright 2018 Spotify AB.
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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream, ObjectStreamException}
import java.util.concurrent.atomic.AtomicReference

private abstract class EmptyOnSerializeAtomicReference[T] extends Serializable {

  private var reference = new AtomicReference[T]()

  reference.set(create())

  def get() : T = {
    reference.get()
  }

  def create() : T

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream): Unit = {
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream): Unit = {
    reference = new AtomicReference[T]()
    reference.set(create())
  }

  @throws[ObjectStreamException]
  private def readObjectNoData(): Unit = {
  }

}
