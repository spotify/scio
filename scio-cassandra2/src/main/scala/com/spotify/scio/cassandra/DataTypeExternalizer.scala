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

package com.spotify.scio.cassandra

import java.lang.{Iterable => JIterable}
import java.util.{Collection => JCollection}

import com.datastax.driver.core.DataType
import com.google.common.collect.{ImmutableList, ImmutableSet, Lists}
import com.twitter.chill._

import scala.collection.JavaConverters._

private[cassandra] object DataTypeExternalizer {
  final def apply(dt: DataType): DataTypeExternalizer = {
    val x = new DataTypeExternalizer
    x.set(dt)
    x
  }
}

private[cassandra] class DataTypeExternalizer extends Externalizer[DataType] {
  override protected def kryo: KryoInstantiator =
    new (DataTypeKryoInstantiator).setReferences(true)
}

private final class DataTypeKryoInstantiator extends EmptyScalaKryoInstantiator {
  override def newKryo: KryoBase = {
    val k = super.newKryo
    k.forSubclass[ImmutableList[Any]](new ImmutableListSerializer[Any])
    k.forSubclass[ImmutableSet[Any]](new ImmutableSetSerializer[Any])
    k
  }
}

private trait ImmutableCollectionSerializer[M] extends KSerializer[M] {
  def readList[T](kser: Kryo, in: Input): JCollection[T] = {
    val size = in.readInt(true)
    val list = Lists.newArrayList[T]()
    (1 to size).foreach(_ => list.add(kser.readClassAndObject(in).asInstanceOf[T]))
    list
  }
  def writeList[T](kser: Kryo, out: Output, obj: JCollection[T]): Unit = {
    out.writeInt(obj.size(), true)
    obj.asScala.foreach(kser.writeClassAndObject(out, _))
  }
}

private final class ImmutableListSerializer[T]
  extends ImmutableCollectionSerializer[ImmutableList[T]] {
  override def read(kser: Kryo, in: Input, cls: Class[ImmutableList[T]]): ImmutableList[T] =
    ImmutableList.copyOf(readList(kser, in): JIterable[T])
  override def write(kser: Kryo, out: Output, obj: ImmutableList[T]): Unit =
    writeList(kser, out, obj)
}

private final class ImmutableSetSerializer[T]
  extends ImmutableCollectionSerializer[ImmutableSet[T]] {
  override def read(kser: Kryo, in: Input, cls: Class[ImmutableSet[T]]): ImmutableSet[T] =
    ImmutableSet.copyOf(readList(kser, in): JIterable[T])
  override def write(kser: Kryo, out: Output, obj: ImmutableSet[T]): Unit =
    writeList(kser, out, obj)
}
