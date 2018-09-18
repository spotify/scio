/*
 * Copyright 2016 Spotify AB.
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

import java.io.{InputStream, OutputStream}
import scala.annotation.implicitNotFound
import org.apache.beam.sdk.coders.{Coder => BCoder, AtomicCoder}
import org.apache.beam.sdk.values.KV
import scala.reflect.ClassTag

@implicitNotFound(
  """
Cannot find a Coder instance for type:

  >> ${T}

  This can happen for a few reasons, but the most common case is that a data
  member somewhere within this type doesn't have a Coder instance in scope. Here are
  some debugging hints:
    - For Option types, ensure that a Coder instance is in scope for the non-Option version.
    - For List and Seq types, ensure that a Coder instance is in scope for a single element.
    - You can check that an instance exists for Coder in the REPL or in your code:
        scala> Coder[Foo]
    And find the missing instance and construct it as needed.
""")
sealed trait Coder[T] extends Serializable
final case class Beam[T] private (beam: BCoder[T]) extends Coder[T]
final case class Fallback[T] private (ct: ClassTag[T]) extends Coder[T]
final case class Transform[A, B] private (c: Coder[A], f: BCoder[A] => Coder[B]) extends Coder[B]
final case class Disjunction[T, Id] private (idCoder: Coder[Id],
                                             id: T => Id,
                                             coder: Map[Id, Coder[T]])
    extends Coder[T]
final case class Record[T] private (cs: Array[(String, Coder[T])]) extends Coder[Array[T]]
// KV are special in beam and need to be serialized using an instance of KvCoder.
final case class KVCoder[K, V] private (koder: Coder[K], voder: Coder[V]) extends Coder[KV[K, V]]

private final case class DisjunctionCoder[T, Id](idCoder: BCoder[Id],
                                                 id: T => Id,
                                                 coders: Map[Id, BCoder[T]])
    extends AtomicCoder[T] {
  def encode(value: T, os: OutputStream): Unit = {
    val i = id(value)
    idCoder.encode(i, os)
    coders(i).encode(value, os)
  }

  def decode(is: InputStream): T = {
    val i = idCoder.decode(is)
    coders(i).decode(is)
  }
}

// XXX: Workaround a NPE deep down the stack in Beam
// info]   java.lang.NullPointerException: null value in entry: T=null
private case class WrappedBCoder[T](u: BCoder[T]) extends BCoder[T] {
  override def toString: String = u.toString
  def encode(value: T, os: OutputStream): Unit = u.encode(value, os)
  def decode(is: InputStream): T = u.decode(is)
  def getCoderArguments(): java.util.List[_ <: BCoder[_]] =
    u.getCoderArguments()
  def verifyDeterministic(): Unit = u.verifyDeterministic()
}

private object WrappedBCoder {
  def create[T](u: BCoder[T]): BCoder[T] =
    u match {
      case WrappedBCoder(_) => u
      case _                => new WrappedBCoder(u)
    }
}

// Coder used internally specifically for Magnolia derived coders.
// It's technically possible to define Product coders only in terms of `Coder.transform`
// This is just faster
private class RecordCoder[T: ClassTag](cs: Array[(String, BCoder[T])])
    extends AtomicCoder[Array[T]] {
  @inline def onErrorMsg[A](msg: => String)(f: => A): A =
    try { f } catch {
      case e: Exception =>
        throw new RuntimeException(msg, e)
    }

  def encode(value: Array[T], os: OutputStream): Unit = {
    var i = 0
    while (i < value.length) {
      val (label, c) = cs(i)
      val v = value(i)
      onErrorMsg(s"Exception while trying to `encode` field ${label} with value ${v}") {
        c.encode(v, os)
      }
      i = i + 1
    }
  }

  def decode(is: InputStream): Array[T] = {
    val vs = new Array[T](cs.length)
    var i = 0
    while (i < cs.length) {
      val (label, c) = cs(i)
      onErrorMsg(s"Exception while trying to `decode` field ${label}") {
        vs.update(i, c.decode(is))
      }
      i = i + 1
    }
    vs
  }
}

sealed trait CoderGrammar {
  def beam[T](beam: BCoder[T]): Coder[T] =
    Beam(beam)
  def kv[K, V](koder: Coder[K], voder: Coder[V]): Coder[KV[K, V]] =
    KVCoder(koder, voder)
  def kryo[T](implicit ct: ClassTag[T]): Coder[T] =
    Fallback[T](ct)
  def transform[A, B](c: Coder[A])(f: BCoder[A] => Coder[B]): Coder[B] =
    Transform(c, f)
  def disjunction[T, Id: Coder](coder: Map[Id, Coder[T]])(id: T => Id): Coder[T] =
    Disjunction(Coder[Id], id, coder)
  def xmap[A, B](c: Coder[A])(f: A => B, t: B => A): Coder[B] = {
    @inline def toB(bc: BCoder[A]) =
      new AtomicCoder[B] {
        def encode(value: B, os: OutputStream): Unit =
          bc.encode(t(value), os)
        def decode(is: InputStream): B =
          f(bc.decode(is))
      }
    Transform[A, B](c, bc => Coder.beam(toB(bc)))
  }
  private[scio] def sequence[T](cs: Array[(String, Coder[T])]): Coder[Array[T]] =
    Record(cs)
}

final object Coder extends CoderGrammar with Implicits {
  def apply[T](implicit c: Coder[T]): Coder[T] = c
}
