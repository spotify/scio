/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.spotify.scio.vendor.chill.algebird

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}

import com.twitter.algebird.{
  AveragedValue,
  DecayedValue,
  HLL,
  HyperLogLog,
  HyperLogLogMonoid,
  Moments,
  QTree
}

import scala.collection.mutable.{Map => MMap}

class AveragedValueSerializer extends KSerializer[AveragedValue] {
  setImmutable(true)
  def write(kser: Kryo, out: Output, s: AveragedValue): Unit = {
    out.writeLong(s.count, true)
    out.writeDouble(s.value)
  }
  def read(kser: Kryo, in: Input, cls: Class[AveragedValue]): AveragedValue =
    AveragedValue(in.readLong(true), in.readDouble)
}

class MomentsSerializer extends KSerializer[Moments] {
  setImmutable(true)
  def write(kser: Kryo, out: Output, s: Moments): Unit = {
    out.writeLong(s.m0, true)
    out.writeDouble(s.m1)
    out.writeDouble(s.m2)
    out.writeDouble(s.m3)
    out.writeDouble(s.m4)
  }
  def read(kser: Kryo, in: Input, cls: Class[Moments]): Moments =
    Moments(in.readLong(true), in.readDouble, in.readDouble, in.readDouble, in.readDouble)
}

class DecayedValueSerializer extends KSerializer[DecayedValue] {
  setImmutable(true)
  def write(kser: Kryo, out: Output, s: DecayedValue): Unit = {
    out.writeDouble(s.value)
    out.writeDouble(s.scaledTime)
  }
  def read(kser: Kryo, in: Input, cls: Class[DecayedValue]): DecayedValue =
    DecayedValue(in.readDouble, in.readDouble)
}

class HLLSerializer extends KSerializer[HLL] {
  setImmutable(true)
  def write(kser: Kryo, out: Output, s: HLL): Unit = {
    val bytes = HyperLogLog.toBytes(s)
    out.writeInt(bytes.size, true)
    out.writeBytes(bytes)
  }
  def read(kser: Kryo, in: Input, cls: Class[HLL]): HLL =
    HyperLogLog.fromBytes(in.readBytes(in.readInt(true)))
}

class HLLMonoidSerializer extends KSerializer[HyperLogLogMonoid] {
  setImmutable(true)
  val hllMonoids: MMap[Int, HyperLogLogMonoid] = MMap[Int, HyperLogLogMonoid]()
  def write(kser: Kryo, out: Output, mon: HyperLogLogMonoid): Unit =
    out.writeInt(mon.bits, true)
  def read(kser: Kryo, in: Input, cls: Class[HyperLogLogMonoid]): HyperLogLogMonoid = {
    val bits = in.readInt(true)
    hllMonoids.getOrElseUpdate(bits, new HyperLogLogMonoid(bits))
  }
}

class QTreeSerializer extends KSerializer[QTree[Any]] {
  setImmutable(true)
  override def read(kryo: Kryo, input: Input, cls: Class[QTree[Any]]): QTree[Any] = {
    val (v1, v2, v3) = (input.readLong(), input.readInt(), input.readLong())
    val v4 = kryo.readClassAndObject(input)
    val v5 = kryo.readClassAndObject(input).asInstanceOf[Option[QTree[Any]]]
    val v6 = kryo.readClassAndObject(input).asInstanceOf[Option[QTree[Any]]]
    QTree(v1, v2, v3, v4, v5, v6)
  }

  override def write(kryo: Kryo, output: Output, obj: QTree[Any]): Unit = {
    output.writeLong(obj._1)
    output.writeInt(obj._2)
    output.writeLong(obj._3)
    kryo.writeClassAndObject(output, obj._4)
    kryo.writeClassAndObject(output, obj._5)
    kryo.writeClassAndObject(output, obj._6)
  }
}
