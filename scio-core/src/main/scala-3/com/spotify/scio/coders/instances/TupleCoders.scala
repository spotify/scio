/*
 * Copyright 2020 Spotify AB.
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

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !! generated with tuplecoders.py
// !! DO NOT EDIT MANUALLY
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

package com.spotify.scio.coders.instances

import java.io.{InputStream, OutputStream}

import com.spotify.scio.coders.{Coder, CoderStackTrace}
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{Coder => BCoder, _}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver

import scala.jdk.CollectionConverters._

final private[coders] class Tuple2Coder[A, B](val ac: BCoder[A], val bc: BCoder[B])
    extends AtomicCoder[(A, B)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TB](msg: => (String, String))(f: => TB): TB =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple2: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
  }
  override def decode(is: InputStream): (A, B) =
    (onErrorMsg("decode" -> "_1")(ac.decode(is)), onErrorMsg("decode" -> "_2")(bc.decode(is)))

  override def toString: String =
    s"Tuple2Coder(_1 -> $ac, _2 -> $bc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List("_1" -> ac, "_2" -> bc)
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals()

  override def structuralValue(value: (A, B)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (ac.structuralValue(value._1), bc.structuralValue(value._2))
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(value._2)

  override def registerByteSizeObserver(value: (A, B), observer: ElementByteSizeObserver): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
  }
}

final private[coders] class Tuple3Coder[A, B, C](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C]
) extends AtomicCoder[(A, B, C)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TC](msg: => (String, String))(f: => TC): TC =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple3: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
  }
  override def decode(is: InputStream): (A, B, C) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is))
    )

  override def toString: String =
    s"Tuple3Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List("_1" -> ac, "_2" -> bc, "_3" -> cc)
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals()

  override def structuralValue(value: (A, B, C)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (ac.structuralValue(value._1), bc.structuralValue(value._2), cc.structuralValue(value._3))
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B, C)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3)

  override def registerByteSizeObserver(
    value: (A, B, C),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
  }
}

final private[coders] class Tuple4Coder[A, B, C, D](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D]
) extends AtomicCoder[(A, B, C, D)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TD](msg: => (String, String))(f: => TD): TD =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple4: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
  }
  override def decode(is: InputStream): (A, B, C, D) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is))
    )

  override def toString: String =
    s"Tuple4Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List("_1" -> ac, "_2" -> bc, "_3" -> cc, "_4" -> dc)
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals()

  override def structuralValue(value: (A, B, C, D)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B, C, D)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
  }
}

final private[coders] class Tuple5Coder[A, B, C, D, E](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E]
) extends AtomicCoder[(A, B, C, D, E)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TE](msg: => (String, String))(f: => TE): TE =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple5: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is))
    )

  override def toString: String =
    s"Tuple5Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List("_1" -> ac, "_2" -> bc, "_3" -> cc, "_4" -> dc, "_5" -> ec)
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B, C, D, E)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5)

  override def registerByteSizeObserver(
    value: (A, B, C, D, E),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
  }
}

final private[coders] class Tuple6Coder[A, B, C, D, E, G](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G]
) extends AtomicCoder[(A, B, C, D, E, G)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TF](msg: => (String, String))(f: => TF): TF =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple6: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E, G), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is))
    )

  override def toString: String =
    s"Tuple6Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List("_1" -> ac, "_2" -> bc, "_3" -> cc, "_4" -> dc, "_5" -> ec, "_6" -> gc)
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B, C, D, E, G)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
  }
}

final private[coders] class Tuple7Coder[A, B, C, D, E, G, H](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H]
) extends AtomicCoder[(A, B, C, D, E, G, H)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TG](msg: => (String, String))(f: => TG): TG =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple7: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E, G, H), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is))
    )

  override def toString: String =
    s"Tuple7Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs =
      List("_1" -> ac, "_2" -> bc, "_3" -> cc, "_4" -> dc, "_5" -> ec, "_6" -> gc, "_7" -> hc)
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B, C, D, E, G, H)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7)

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
  }
}

final private[coders] class Tuple8Coder[A, B, C, D, E, G, H, I](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I]
) extends AtomicCoder[(A, B, C, D, E, G, H, I)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TH](msg: => (String, String))(f: => TH): TH =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple8: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E, G, H, I), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is))
    )

  override def toString: String =
    s"Tuple8Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B, C, D, E, G, H, I)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
  }
}

final private[coders] class Tuple9Coder[A, B, C, D, E, G, H, I, J](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TI](msg: => (String, String))(f: => TI): TI =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple9: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E, G, H, I, J), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is))
    )

  override def toString: String =
    s"Tuple9Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I, J)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B, C, D, E, G, H, I, J)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9)

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
  }
}

final private[coders] class Tuple10Coder[A, B, C, D, E, G, H, I, J, K](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TJ](msg: => (String, String))(f: => TJ): TJ =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple10: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E, G, H, I, J, K), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is))
    )

  override def toString: String =
    s"Tuple10Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I, J, K)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B, C, D, E, G, H, I, J, K)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
  }
}

final private[coders] class Tuple11Coder[A, B, C, D, E, G, H, I, J, K, L](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TK](msg: => (String, String))(f: => TK): TK =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple11: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E, G, H, I, J, K, L), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K, L) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is))
    )

  override def toString: String =
    s"Tuple11Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I, J, K, L)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B, C, D, E, G, H, I, J, K, L)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11)

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
  }
}

final private[coders] class Tuple12Coder[A, B, C, D, E, G, H, I, J, K, L, M](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TL](msg: => (String, String))(f: => TL): TL =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple12: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E, G, H, I, J, K, L, M), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K, L, M) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is))
    )

  override def toString: String =
    s"Tuple12Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I, J, K, L, M)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
  }
}

final private[coders] class Tuple13Coder[A, B, C, D, E, G, H, I, J, K, L, M, N](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TM](msg: => (String, String))(f: => TM): TM =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple13: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E, G, H, I, J, K, L, M, N), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K, L, M, N) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is))
    )

  override def toString: String =
    s"Tuple13Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I, J, K, L, M, N)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13)

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
  }
}

final private[coders] class Tuple14Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N],
  val oc: BCoder[O]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TN](msg: => (String, String))(f: => TN): TN =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple14: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
    onErrorMsg("encode" -> "_14")(oc.encode(value._14, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K, L, M, N, O) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is)),
      onErrorMsg("decode" -> "_14")(oc.decode(is))
    )

  override def toString: String =
    s"Tuple14Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc, _14 -> $oc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc,
      "_14" -> oc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals() && oc.consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13),
        oc.structuralValue(value._14)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13) && oc.isRegisterByteSizeObserverCheap(
      value._14
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
    oc.registerByteSizeObserver(value._14, observer)
  }
}

final private[coders] class Tuple15Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N],
  val oc: BCoder[O],
  val pc: BCoder[P]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TO](msg: => (String, String))(f: => TO): TO =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple15: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P),
    os: OutputStream
  ): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
    onErrorMsg("encode" -> "_14")(oc.encode(value._14, os))
    onErrorMsg("encode" -> "_15")(pc.encode(value._15, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is)),
      onErrorMsg("decode" -> "_14")(oc.decode(is)),
      onErrorMsg("decode" -> "_15")(pc.decode(is))
    )

  override def toString: String =
    s"Tuple15Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc, _14 -> $oc, _15 -> $pc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc,
      "_14" -> oc,
      "_15" -> pc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals() && oc.consistentWithEquals() && pc.consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13),
        oc.structuralValue(value._14),
        pc.structuralValue(value._15)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13) && oc.isRegisterByteSizeObserverCheap(
      value._14
    ) && pc.isRegisterByteSizeObserverCheap(value._15)

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
    oc.registerByteSizeObserver(value._14, observer)
    pc.registerByteSizeObserver(value._15, observer)
  }
}

final private[coders] class Tuple16Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N],
  val oc: BCoder[O],
  val pc: BCoder[P],
  val qc: BCoder[Q]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TP](msg: => (String, String))(f: => TP): TP =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple16: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q),
    os: OutputStream
  ): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
    onErrorMsg("encode" -> "_14")(oc.encode(value._14, os))
    onErrorMsg("encode" -> "_15")(pc.encode(value._15, os))
    onErrorMsg("encode" -> "_16")(qc.encode(value._16, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is)),
      onErrorMsg("decode" -> "_14")(oc.decode(is)),
      onErrorMsg("decode" -> "_15")(pc.decode(is)),
      onErrorMsg("decode" -> "_16")(qc.decode(is))
    )

  override def toString: String =
    s"Tuple16Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc, _14 -> $oc, _15 -> $pc, _16 -> $qc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc,
      "_14" -> oc,
      "_15" -> pc,
      "_16" -> qc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals() && oc.consistentWithEquals() && pc.consistentWithEquals() && qc
      .consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13),
        oc.structuralValue(value._14),
        pc.structuralValue(value._15),
        qc.structuralValue(value._16)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13) && oc.isRegisterByteSizeObserverCheap(
      value._14
    ) && pc.isRegisterByteSizeObserverCheap(value._15) && qc.isRegisterByteSizeObserverCheap(
      value._16
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
    oc.registerByteSizeObserver(value._14, observer)
    pc.registerByteSizeObserver(value._15, observer)
    qc.registerByteSizeObserver(value._16, observer)
  }
}

final private[coders] class Tuple17Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N],
  val oc: BCoder[O],
  val pc: BCoder[P],
  val qc: BCoder[Q],
  val rc: BCoder[R]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TQ](msg: => (String, String))(f: => TQ): TQ =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple17: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R),
    os: OutputStream
  ): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
    onErrorMsg("encode" -> "_14")(oc.encode(value._14, os))
    onErrorMsg("encode" -> "_15")(pc.encode(value._15, os))
    onErrorMsg("encode" -> "_16")(qc.encode(value._16, os))
    onErrorMsg("encode" -> "_17")(rc.encode(value._17, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is)),
      onErrorMsg("decode" -> "_14")(oc.decode(is)),
      onErrorMsg("decode" -> "_15")(pc.decode(is)),
      onErrorMsg("decode" -> "_16")(qc.decode(is)),
      onErrorMsg("decode" -> "_17")(rc.decode(is))
    )

  override def toString: String =
    s"Tuple17Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc, _14 -> $oc, _15 -> $pc, _16 -> $qc, _17 -> $rc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc,
      "_14" -> oc,
      "_15" -> pc,
      "_16" -> qc,
      "_17" -> rc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals() && oc.consistentWithEquals() && pc.consistentWithEquals() && qc
      .consistentWithEquals() && rc.consistentWithEquals()

  override def structuralValue(value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13),
        oc.structuralValue(value._14),
        pc.structuralValue(value._15),
        qc.structuralValue(value._16),
        rc.structuralValue(value._17)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13) && oc.isRegisterByteSizeObserverCheap(
      value._14
    ) && pc.isRegisterByteSizeObserverCheap(value._15) && qc.isRegisterByteSizeObserverCheap(
      value._16
    ) && rc.isRegisterByteSizeObserverCheap(value._17)

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
    oc.registerByteSizeObserver(value._14, observer)
    pc.registerByteSizeObserver(value._15, observer)
    qc.registerByteSizeObserver(value._16, observer)
    rc.registerByteSizeObserver(value._17, observer)
  }
}

final private[coders] class Tuple18Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N],
  val oc: BCoder[O],
  val pc: BCoder[P],
  val qc: BCoder[Q],
  val rc: BCoder[R],
  val sc: BCoder[S]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TR](msg: => (String, String))(f: => TR): TR =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple18: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S),
    os: OutputStream
  ): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
    onErrorMsg("encode" -> "_14")(oc.encode(value._14, os))
    onErrorMsg("encode" -> "_15")(pc.encode(value._15, os))
    onErrorMsg("encode" -> "_16")(qc.encode(value._16, os))
    onErrorMsg("encode" -> "_17")(rc.encode(value._17, os))
    onErrorMsg("encode" -> "_18")(sc.encode(value._18, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is)),
      onErrorMsg("decode" -> "_14")(oc.decode(is)),
      onErrorMsg("decode" -> "_15")(pc.decode(is)),
      onErrorMsg("decode" -> "_16")(qc.decode(is)),
      onErrorMsg("decode" -> "_17")(rc.decode(is)),
      onErrorMsg("decode" -> "_18")(sc.decode(is))
    )

  override def toString: String =
    s"Tuple18Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc, _14 -> $oc, _15 -> $pc, _16 -> $qc, _17 -> $rc, _18 -> $sc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc,
      "_14" -> oc,
      "_15" -> pc,
      "_16" -> qc,
      "_17" -> rc,
      "_18" -> sc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals() && oc.consistentWithEquals() && pc.consistentWithEquals() && qc
      .consistentWithEquals() && rc.consistentWithEquals() && sc.consistentWithEquals()

  override def structuralValue(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)
  ): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13),
        oc.structuralValue(value._14),
        pc.structuralValue(value._15),
        qc.structuralValue(value._16),
        rc.structuralValue(value._17),
        sc.structuralValue(value._18)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13) && oc.isRegisterByteSizeObserverCheap(
      value._14
    ) && pc.isRegisterByteSizeObserverCheap(value._15) && qc.isRegisterByteSizeObserverCheap(
      value._16
    ) && rc.isRegisterByteSizeObserverCheap(value._17) && sc.isRegisterByteSizeObserverCheap(
      value._18
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
    oc.registerByteSizeObserver(value._14, observer)
    pc.registerByteSizeObserver(value._15, observer)
    qc.registerByteSizeObserver(value._16, observer)
    rc.registerByteSizeObserver(value._17, observer)
    sc.registerByteSizeObserver(value._18, observer)
  }
}

final private[coders] class Tuple19Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N],
  val oc: BCoder[O],
  val pc: BCoder[P],
  val qc: BCoder[Q],
  val rc: BCoder[R],
  val sc: BCoder[S],
  val tc: BCoder[T]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TS](msg: => (String, String))(f: => TS): TS =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple19: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T),
    os: OutputStream
  ): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
    onErrorMsg("encode" -> "_14")(oc.encode(value._14, os))
    onErrorMsg("encode" -> "_15")(pc.encode(value._15, os))
    onErrorMsg("encode" -> "_16")(qc.encode(value._16, os))
    onErrorMsg("encode" -> "_17")(rc.encode(value._17, os))
    onErrorMsg("encode" -> "_18")(sc.encode(value._18, os))
    onErrorMsg("encode" -> "_19")(tc.encode(value._19, os))
  }
  override def decode(is: InputStream): (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is)),
      onErrorMsg("decode" -> "_14")(oc.decode(is)),
      onErrorMsg("decode" -> "_15")(pc.decode(is)),
      onErrorMsg("decode" -> "_16")(qc.decode(is)),
      onErrorMsg("decode" -> "_17")(rc.decode(is)),
      onErrorMsg("decode" -> "_18")(sc.decode(is)),
      onErrorMsg("decode" -> "_19")(tc.decode(is))
    )

  override def toString: String =
    s"Tuple19Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc, _14 -> $oc, _15 -> $pc, _16 -> $qc, _17 -> $rc, _18 -> $sc, _19 -> $tc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc,
      "_14" -> oc,
      "_15" -> pc,
      "_16" -> qc,
      "_17" -> rc,
      "_18" -> sc,
      "_19" -> tc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals() && oc.consistentWithEquals() && pc.consistentWithEquals() && qc
      .consistentWithEquals() && rc.consistentWithEquals() && sc.consistentWithEquals() && tc
      .consistentWithEquals()

  override def structuralValue(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)
  ): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13),
        oc.structuralValue(value._14),
        pc.structuralValue(value._15),
        qc.structuralValue(value._16),
        rc.structuralValue(value._17),
        sc.structuralValue(value._18),
        tc.structuralValue(value._19)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13) && oc.isRegisterByteSizeObserverCheap(
      value._14
    ) && pc.isRegisterByteSizeObserverCheap(value._15) && qc.isRegisterByteSizeObserverCheap(
      value._16
    ) && rc.isRegisterByteSizeObserverCheap(value._17) && sc.isRegisterByteSizeObserverCheap(
      value._18
    ) && tc.isRegisterByteSizeObserverCheap(value._19)

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
    oc.registerByteSizeObserver(value._14, observer)
    pc.registerByteSizeObserver(value._15, observer)
    qc.registerByteSizeObserver(value._16, observer)
    rc.registerByteSizeObserver(value._17, observer)
    sc.registerByteSizeObserver(value._18, observer)
    tc.registerByteSizeObserver(value._19, observer)
  }
}

final private[coders] class Tuple20Coder[
  A,
  B,
  C,
  D,
  E,
  G,
  H,
  I,
  J,
  K,
  L,
  M,
  N,
  O,
  P,
  Q,
  R,
  S,
  T,
  U
](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N],
  val oc: BCoder[O],
  val pc: BCoder[P],
  val qc: BCoder[Q],
  val rc: BCoder[R],
  val sc: BCoder[S],
  val tc: BCoder[T],
  val uc: BCoder[U]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TT](msg: => (String, String))(f: => TT): TT =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple20: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U),
    os: OutputStream
  ): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
    onErrorMsg("encode" -> "_14")(oc.encode(value._14, os))
    onErrorMsg("encode" -> "_15")(pc.encode(value._15, os))
    onErrorMsg("encode" -> "_16")(qc.encode(value._16, os))
    onErrorMsg("encode" -> "_17")(rc.encode(value._17, os))
    onErrorMsg("encode" -> "_18")(sc.encode(value._18, os))
    onErrorMsg("encode" -> "_19")(tc.encode(value._19, os))
    onErrorMsg("encode" -> "_20")(uc.encode(value._20, os))
  }
  override def decode(
    is: InputStream
  ): (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is)),
      onErrorMsg("decode" -> "_14")(oc.decode(is)),
      onErrorMsg("decode" -> "_15")(pc.decode(is)),
      onErrorMsg("decode" -> "_16")(qc.decode(is)),
      onErrorMsg("decode" -> "_17")(rc.decode(is)),
      onErrorMsg("decode" -> "_18")(sc.decode(is)),
      onErrorMsg("decode" -> "_19")(tc.decode(is)),
      onErrorMsg("decode" -> "_20")(uc.decode(is))
    )

  override def toString: String =
    s"Tuple20Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc, _14 -> $oc, _15 -> $pc, _16 -> $qc, _17 -> $rc, _18 -> $sc, _19 -> $tc, _20 -> $uc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc,
      "_14" -> oc,
      "_15" -> pc,
      "_16" -> qc,
      "_17" -> rc,
      "_18" -> sc,
      "_19" -> tc,
      "_20" -> uc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals() && oc.consistentWithEquals() && pc.consistentWithEquals() && qc
      .consistentWithEquals() && rc.consistentWithEquals() && sc.consistentWithEquals() && tc
      .consistentWithEquals() && uc.consistentWithEquals()

  override def structuralValue(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)
  ): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13),
        oc.structuralValue(value._14),
        pc.structuralValue(value._15),
        qc.structuralValue(value._16),
        rc.structuralValue(value._17),
        sc.structuralValue(value._18),
        tc.structuralValue(value._19),
        uc.structuralValue(value._20)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13) && oc.isRegisterByteSizeObserverCheap(
      value._14
    ) && pc.isRegisterByteSizeObserverCheap(value._15) && qc.isRegisterByteSizeObserverCheap(
      value._16
    ) && rc.isRegisterByteSizeObserverCheap(value._17) && sc.isRegisterByteSizeObserverCheap(
      value._18
    ) && tc.isRegisterByteSizeObserverCheap(value._19) && uc.isRegisterByteSizeObserverCheap(
      value._20
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
    oc.registerByteSizeObserver(value._14, observer)
    pc.registerByteSizeObserver(value._15, observer)
    qc.registerByteSizeObserver(value._16, observer)
    rc.registerByteSizeObserver(value._17, observer)
    sc.registerByteSizeObserver(value._18, observer)
    tc.registerByteSizeObserver(value._19, observer)
    uc.registerByteSizeObserver(value._20, observer)
  }
}

final private[coders] class Tuple21Coder[
  A,
  B,
  C,
  D,
  E,
  G,
  H,
  I,
  J,
  K,
  L,
  M,
  N,
  O,
  P,
  Q,
  R,
  S,
  T,
  U,
  V
](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N],
  val oc: BCoder[O],
  val pc: BCoder[P],
  val qc: BCoder[Q],
  val rc: BCoder[R],
  val sc: BCoder[S],
  val tc: BCoder[T],
  val uc: BCoder[U],
  val vc: BCoder[V]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TU](msg: => (String, String))(f: => TU): TU =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple21: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V),
    os: OutputStream
  ): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
    onErrorMsg("encode" -> "_14")(oc.encode(value._14, os))
    onErrorMsg("encode" -> "_15")(pc.encode(value._15, os))
    onErrorMsg("encode" -> "_16")(qc.encode(value._16, os))
    onErrorMsg("encode" -> "_17")(rc.encode(value._17, os))
    onErrorMsg("encode" -> "_18")(sc.encode(value._18, os))
    onErrorMsg("encode" -> "_19")(tc.encode(value._19, os))
    onErrorMsg("encode" -> "_20")(uc.encode(value._20, os))
    onErrorMsg("encode" -> "_21")(vc.encode(value._21, os))
  }
  override def decode(
    is: InputStream
  ): (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is)),
      onErrorMsg("decode" -> "_14")(oc.decode(is)),
      onErrorMsg("decode" -> "_15")(pc.decode(is)),
      onErrorMsg("decode" -> "_16")(qc.decode(is)),
      onErrorMsg("decode" -> "_17")(rc.decode(is)),
      onErrorMsg("decode" -> "_18")(sc.decode(is)),
      onErrorMsg("decode" -> "_19")(tc.decode(is)),
      onErrorMsg("decode" -> "_20")(uc.decode(is)),
      onErrorMsg("decode" -> "_21")(vc.decode(is))
    )

  override def toString: String =
    s"Tuple21Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc, _14 -> $oc, _15 -> $pc, _16 -> $qc, _17 -> $rc, _18 -> $sc, _19 -> $tc, _20 -> $uc, _21 -> $vc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc,
      "_14" -> oc,
      "_15" -> pc,
      "_16" -> qc,
      "_17" -> rc,
      "_18" -> sc,
      "_19" -> tc,
      "_20" -> uc,
      "_21" -> vc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals() && oc.consistentWithEquals() && pc.consistentWithEquals() && qc
      .consistentWithEquals() && rc.consistentWithEquals() && sc.consistentWithEquals() && tc
      .consistentWithEquals() && uc.consistentWithEquals() && vc.consistentWithEquals()

  override def structuralValue(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)
  ): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13),
        oc.structuralValue(value._14),
        pc.structuralValue(value._15),
        qc.structuralValue(value._16),
        rc.structuralValue(value._17),
        sc.structuralValue(value._18),
        tc.structuralValue(value._19),
        uc.structuralValue(value._20),
        vc.structuralValue(value._21)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13) && oc.isRegisterByteSizeObserverCheap(
      value._14
    ) && pc.isRegisterByteSizeObserverCheap(value._15) && qc.isRegisterByteSizeObserverCheap(
      value._16
    ) && rc.isRegisterByteSizeObserverCheap(value._17) && sc.isRegisterByteSizeObserverCheap(
      value._18
    ) && tc.isRegisterByteSizeObserverCheap(value._19) && uc.isRegisterByteSizeObserverCheap(
      value._20
    ) && vc.isRegisterByteSizeObserverCheap(value._21)

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
    oc.registerByteSizeObserver(value._14, observer)
    pc.registerByteSizeObserver(value._15, observer)
    qc.registerByteSizeObserver(value._16, observer)
    rc.registerByteSizeObserver(value._17, observer)
    sc.registerByteSizeObserver(value._18, observer)
    tc.registerByteSizeObserver(value._19, observer)
    uc.registerByteSizeObserver(value._20, observer)
    vc.registerByteSizeObserver(value._21, observer)
  }
}

final private[coders] class Tuple22Coder[
  A,
  B,
  C,
  D,
  E,
  G,
  H,
  I,
  J,
  K,
  L,
  M,
  N,
  O,
  P,
  Q,
  R,
  S,
  T,
  U,
  V,
  W
](
  val ac: BCoder[A],
  val bc: BCoder[B],
  val cc: BCoder[C],
  val dc: BCoder[D],
  val ec: BCoder[E],
  val gc: BCoder[G],
  val hc: BCoder[H],
  val ic: BCoder[I],
  val jc: BCoder[J],
  val kc: BCoder[K],
  val lc: BCoder[L],
  val mc: BCoder[M],
  val nc: BCoder[N],
  val oc: BCoder[O],
  val pc: BCoder[P],
  val qc: BCoder[Q],
  val rc: BCoder[R],
  val sc: BCoder[S],
  val tc: BCoder[T],
  val uc: BCoder[U],
  val vc: BCoder[V],
  val wc: BCoder[W]
) extends AtomicCoder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)] {
  private[this] val materializationStackTrace: Array[StackTraceElement] = CoderStackTrace.prepare

  @inline def onErrorMsg[TV](msg: => (String, String))(f: => TV): TV =
    try {
      f
    } catch {
      case e: Exception =>
        // allow Flink memory management, see WrappedBCoder#catching comment.
        throw CoderStackTrace.append(
          e,
          Some(
            s"Exception while trying to `${msg._1}` an instance" +
              s" of Tuple22: Can't decode field ${msg._2}"
          ),
          materializationStackTrace
        )
    }

  override def encode(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W),
    os: OutputStream
  ): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
    onErrorMsg("encode" -> "_3")(cc.encode(value._3, os))
    onErrorMsg("encode" -> "_4")(dc.encode(value._4, os))
    onErrorMsg("encode" -> "_5")(ec.encode(value._5, os))
    onErrorMsg("encode" -> "_6")(gc.encode(value._6, os))
    onErrorMsg("encode" -> "_7")(hc.encode(value._7, os))
    onErrorMsg("encode" -> "_8")(ic.encode(value._8, os))
    onErrorMsg("encode" -> "_9")(jc.encode(value._9, os))
    onErrorMsg("encode" -> "_10")(kc.encode(value._10, os))
    onErrorMsg("encode" -> "_11")(lc.encode(value._11, os))
    onErrorMsg("encode" -> "_12")(mc.encode(value._12, os))
    onErrorMsg("encode" -> "_13")(nc.encode(value._13, os))
    onErrorMsg("encode" -> "_14")(oc.encode(value._14, os))
    onErrorMsg("encode" -> "_15")(pc.encode(value._15, os))
    onErrorMsg("encode" -> "_16")(qc.encode(value._16, os))
    onErrorMsg("encode" -> "_17")(rc.encode(value._17, os))
    onErrorMsg("encode" -> "_18")(sc.encode(value._18, os))
    onErrorMsg("encode" -> "_19")(tc.encode(value._19, os))
    onErrorMsg("encode" -> "_20")(uc.encode(value._20, os))
    onErrorMsg("encode" -> "_21")(vc.encode(value._21, os))
    onErrorMsg("encode" -> "_22")(wc.encode(value._22, os))
  }
  override def decode(
    is: InputStream
  ): (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W) =
    (
      onErrorMsg("decode" -> "_1")(ac.decode(is)),
      onErrorMsg("decode" -> "_2")(bc.decode(is)),
      onErrorMsg("decode" -> "_3")(cc.decode(is)),
      onErrorMsg("decode" -> "_4")(dc.decode(is)),
      onErrorMsg("decode" -> "_5")(ec.decode(is)),
      onErrorMsg("decode" -> "_6")(gc.decode(is)),
      onErrorMsg("decode" -> "_7")(hc.decode(is)),
      onErrorMsg("decode" -> "_8")(ic.decode(is)),
      onErrorMsg("decode" -> "_9")(jc.decode(is)),
      onErrorMsg("decode" -> "_10")(kc.decode(is)),
      onErrorMsg("decode" -> "_11")(lc.decode(is)),
      onErrorMsg("decode" -> "_12")(mc.decode(is)),
      onErrorMsg("decode" -> "_13")(nc.decode(is)),
      onErrorMsg("decode" -> "_14")(oc.decode(is)),
      onErrorMsg("decode" -> "_15")(pc.decode(is)),
      onErrorMsg("decode" -> "_16")(qc.decode(is)),
      onErrorMsg("decode" -> "_17")(rc.decode(is)),
      onErrorMsg("decode" -> "_18")(sc.decode(is)),
      onErrorMsg("decode" -> "_19")(tc.decode(is)),
      onErrorMsg("decode" -> "_20")(uc.decode(is)),
      onErrorMsg("decode" -> "_21")(vc.decode(is)),
      onErrorMsg("decode" -> "_22")(wc.decode(is))
    )

  override def toString: String =
    s"Tuple22Coder(_1 -> $ac, _2 -> $bc, _3 -> $cc, _4 -> $dc, _5 -> $ec, _6 -> $gc, _7 -> $hc, _8 -> $ic, _9 -> $jc, _10 -> $kc, _11 -> $lc, _12 -> $mc, _13 -> $nc, _14 -> $oc, _15 -> $pc, _16 -> $qc, _17 -> $rc, _18 -> $sc, _19 -> $tc, _20 -> $uc, _21 -> $vc, _22 -> $wc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List(
      "_1" -> ac,
      "_2" -> bc,
      "_3" -> cc,
      "_4" -> dc,
      "_5" -> ec,
      "_6" -> gc,
      "_7" -> hc,
      "_8" -> ic,
      "_9" -> jc,
      "_10" -> kc,
      "_11" -> lc,
      "_12" -> mc,
      "_13" -> nc,
      "_14" -> oc,
      "_15" -> pc,
      "_16" -> qc,
      "_17" -> rc,
      "_18" -> sc,
      "_19" -> tc,
      "_20" -> uc,
      "_21" -> vc,
      "_22" -> wc
    )
    val problems = cs.flatMap { case (label, c) =>
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"field $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals() && cc.consistentWithEquals() && dc
      .consistentWithEquals() && ec.consistentWithEquals() && gc.consistentWithEquals() && hc
      .consistentWithEquals() && ic.consistentWithEquals() && jc.consistentWithEquals() && kc
      .consistentWithEquals() && lc.consistentWithEquals() && mc.consistentWithEquals() && nc
      .consistentWithEquals() && oc.consistentWithEquals() && pc.consistentWithEquals() && qc
      .consistentWithEquals() && rc.consistentWithEquals() && sc.consistentWithEquals() && tc
      .consistentWithEquals() && uc.consistentWithEquals() && vc.consistentWithEquals() && wc
      .consistentWithEquals()

  override def structuralValue(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)
  ): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (
        ac.structuralValue(value._1),
        bc.structuralValue(value._2),
        cc.structuralValue(value._3),
        dc.structuralValue(value._4),
        ec.structuralValue(value._5),
        gc.structuralValue(value._6),
        hc.structuralValue(value._7),
        ic.structuralValue(value._8),
        jc.structuralValue(value._9),
        kc.structuralValue(value._10),
        lc.structuralValue(value._11),
        mc.structuralValue(value._12),
        nc.structuralValue(value._13),
        oc.structuralValue(value._14),
        pc.structuralValue(value._15),
        qc.structuralValue(value._16),
        rc.structuralValue(value._17),
        sc.structuralValue(value._18),
        tc.structuralValue(value._19),
        uc.structuralValue(value._20),
        vc.structuralValue(value._21),
        wc.structuralValue(value._22)
      )
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)
  ): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(
      value._2
    ) && cc.isRegisterByteSizeObserverCheap(value._3) && dc.isRegisterByteSizeObserverCheap(
      value._4
    ) && ec.isRegisterByteSizeObserverCheap(value._5) && gc.isRegisterByteSizeObserverCheap(
      value._6
    ) && hc.isRegisterByteSizeObserverCheap(value._7) && ic.isRegisterByteSizeObserverCheap(
      value._8
    ) && jc.isRegisterByteSizeObserverCheap(value._9) && kc.isRegisterByteSizeObserverCheap(
      value._10
    ) && lc.isRegisterByteSizeObserverCheap(value._11) && mc.isRegisterByteSizeObserverCheap(
      value._12
    ) && nc.isRegisterByteSizeObserverCheap(value._13) && oc.isRegisterByteSizeObserverCheap(
      value._14
    ) && pc.isRegisterByteSizeObserverCheap(value._15) && qc.isRegisterByteSizeObserverCheap(
      value._16
    ) && rc.isRegisterByteSizeObserverCheap(value._17) && sc.isRegisterByteSizeObserverCheap(
      value._18
    ) && tc.isRegisterByteSizeObserverCheap(value._19) && uc.isRegisterByteSizeObserverCheap(
      value._20
    ) && vc.isRegisterByteSizeObserverCheap(value._21) && wc.isRegisterByteSizeObserverCheap(
      value._22
    )

  override def registerByteSizeObserver(
    value: (A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W),
    observer: ElementByteSizeObserver
  ): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
    cc.registerByteSizeObserver(value._3, observer)
    dc.registerByteSizeObserver(value._4, observer)
    ec.registerByteSizeObserver(value._5, observer)
    gc.registerByteSizeObserver(value._6, observer)
    hc.registerByteSizeObserver(value._7, observer)
    ic.registerByteSizeObserver(value._8, observer)
    jc.registerByteSizeObserver(value._9, observer)
    kc.registerByteSizeObserver(value._10, observer)
    lc.registerByteSizeObserver(value._11, observer)
    mc.registerByteSizeObserver(value._12, observer)
    nc.registerByteSizeObserver(value._13, observer)
    oc.registerByteSizeObserver(value._14, observer)
    pc.registerByteSizeObserver(value._15, observer)
    qc.registerByteSizeObserver(value._16, observer)
    rc.registerByteSizeObserver(value._17, observer)
    sc.registerByteSizeObserver(value._18, observer)
    tc.registerByteSizeObserver(value._19, observer)
    uc.registerByteSizeObserver(value._20, observer)
    vc.registerByteSizeObserver(value._21, observer)
    wc.registerByteSizeObserver(value._22, observer)
  }
}

trait TupleCoders {

  implicit def tuple2Coder[A, B](implicit CA: Coder[A], CB: Coder[B]): Coder[(A, B)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB)(bc => Coder.beam(new Tuple2Coder[A, B](ac, bc)))
    }

  implicit def tuple3Coder[A, B, C](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C]
  ): Coder[(A, B, C)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC)(cc => Coder.beam(new Tuple3Coder[A, B, C](ac, bc, cc)))
      }
    }

  implicit def tuple4Coder[A, B, C, D](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D]
  ): Coder[(A, B, C, D)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD)(dc => Coder.beam(new Tuple4Coder[A, B, C, D](ac, bc, cc, dc)))
        }
      }
    }

  implicit def tuple5Coder[A, B, C, D, E](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E]
  ): Coder[(A, B, C, D, E)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE)(ec =>
              Coder.beam(new Tuple5Coder[A, B, C, D, E](ac, bc, cc, dc, ec))
            )
          }
        }
      }
    }

  implicit def tuple6Coder[A, B, C, D, E, G](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G]
  ): Coder[(A, B, C, D, E, G)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG)(gc =>
                Coder.beam(new Tuple6Coder[A, B, C, D, E, G](ac, bc, cc, dc, ec, gc))
              )
            }
          }
        }
      }
    }

  implicit def tuple7Coder[A, B, C, D, E, G, H](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H]
  ): Coder[(A, B, C, D, E, G, H)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH)(hc =>
                  Coder.beam(new Tuple7Coder[A, B, C, D, E, G, H](ac, bc, cc, dc, ec, gc, hc))
                )
              }
            }
          }
        }
      }
    }

  implicit def tuple8Coder[A, B, C, D, E, G, H, I](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I]
  ): Coder[(A, B, C, D, E, G, H, I)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI)(ic =>
                    Coder.beam(
                      new Tuple8Coder[A, B, C, D, E, G, H, I](ac, bc, cc, dc, ec, gc, hc, ic)
                    )
                  )
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple9Coder[A, B, C, D, E, G, H, I, J](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J]
  ): Coder[(A, B, C, D, E, G, H, I, J)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ)(jc =>
                      Coder.beam(
                        new Tuple9Coder[A, B, C, D, E, G, H, I, J](
                          ac,
                          bc,
                          cc,
                          dc,
                          ec,
                          gc,
                          hc,
                          ic,
                          jc
                        )
                      )
                    )
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple10Coder[A, B, C, D, E, G, H, I, J, K](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K]
  ): Coder[(A, B, C, D, E, G, H, I, J, K)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK)(kc =>
                        Coder.beam(
                          new Tuple10Coder[A, B, C, D, E, G, H, I, J, K](
                            ac,
                            bc,
                            cc,
                            dc,
                            ec,
                            gc,
                            hc,
                            ic,
                            jc,
                            kc
                          )
                        )
                      )
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple11Coder[A, B, C, D, E, G, H, I, J, K, L](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL)(lc =>
                          Coder.beam(
                            new Tuple11Coder[A, B, C, D, E, G, H, I, J, K, L](
                              ac,
                              bc,
                              cc,
                              dc,
                              ec,
                              gc,
                              hc,
                              ic,
                              jc,
                              kc,
                              lc
                            )
                          )
                        )
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple12Coder[A, B, C, D, E, G, H, I, J, K, L, M](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM)(mc =>
                            Coder.beam(
                              new Tuple12Coder[A, B, C, D, E, G, H, I, J, K, L, M](
                                ac,
                                bc,
                                cc,
                                dc,
                                ec,
                                gc,
                                hc,
                                ic,
                                jc,
                                kc,
                                lc,
                                mc
                              )
                            )
                          )
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple13Coder[A, B, C, D, E, G, H, I, J, K, L, M, N](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN)(nc =>
                              Coder.beam(
                                new Tuple13Coder[A, B, C, D, E, G, H, I, J, K, L, M, N](
                                  ac,
                                  bc,
                                  cc,
                                  dc,
                                  ec,
                                  gc,
                                  hc,
                                  ic,
                                  jc,
                                  kc,
                                  lc,
                                  mc,
                                  nc
                                )
                              )
                            )
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple14Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N],
    CO: Coder[O]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN) { nc =>
                              Coder.transform(CO)(oc =>
                                Coder.beam(
                                  new Tuple14Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O](
                                    ac,
                                    bc,
                                    cc,
                                    dc,
                                    ec,
                                    gc,
                                    hc,
                                    ic,
                                    jc,
                                    kc,
                                    lc,
                                    mc,
                                    nc,
                                    oc
                                  )
                                )
                              )
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple15Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N],
    CO: Coder[O],
    CP: Coder[P]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN) { nc =>
                              Coder.transform(CO) { oc =>
                                Coder.transform(CP)(pc =>
                                  Coder.beam(
                                    new Tuple15Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P](
                                      ac,
                                      bc,
                                      cc,
                                      dc,
                                      ec,
                                      gc,
                                      hc,
                                      ic,
                                      jc,
                                      kc,
                                      lc,
                                      mc,
                                      nc,
                                      oc,
                                      pc
                                    )
                                  )
                                )
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple16Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N],
    CO: Coder[O],
    CP: Coder[P],
    CQ: Coder[Q]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN) { nc =>
                              Coder.transform(CO) { oc =>
                                Coder.transform(CP) { pc =>
                                  Coder.transform(CQ)(qc =>
                                    Coder.beam(
                                      new Tuple16Coder[
                                        A,
                                        B,
                                        C,
                                        D,
                                        E,
                                        G,
                                        H,
                                        I,
                                        J,
                                        K,
                                        L,
                                        M,
                                        N,
                                        O,
                                        P,
                                        Q
                                      ](
                                        ac,
                                        bc,
                                        cc,
                                        dc,
                                        ec,
                                        gc,
                                        hc,
                                        ic,
                                        jc,
                                        kc,
                                        lc,
                                        mc,
                                        nc,
                                        oc,
                                        pc,
                                        qc
                                      )
                                    )
                                  )
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple17Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N],
    CO: Coder[O],
    CP: Coder[P],
    CQ: Coder[Q],
    CR: Coder[R]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN) { nc =>
                              Coder.transform(CO) { oc =>
                                Coder.transform(CP) { pc =>
                                  Coder.transform(CQ) { qc =>
                                    Coder.transform(CR)(rc =>
                                      Coder.beam(
                                        new Tuple17Coder[
                                          A,
                                          B,
                                          C,
                                          D,
                                          E,
                                          G,
                                          H,
                                          I,
                                          J,
                                          K,
                                          L,
                                          M,
                                          N,
                                          O,
                                          P,
                                          Q,
                                          R
                                        ](
                                          ac,
                                          bc,
                                          cc,
                                          dc,
                                          ec,
                                          gc,
                                          hc,
                                          ic,
                                          jc,
                                          kc,
                                          lc,
                                          mc,
                                          nc,
                                          oc,
                                          pc,
                                          qc,
                                          rc
                                        )
                                      )
                                    )
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple18Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N],
    CO: Coder[O],
    CP: Coder[P],
    CQ: Coder[Q],
    CR: Coder[R],
    CS: Coder[S]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN) { nc =>
                              Coder.transform(CO) { oc =>
                                Coder.transform(CP) { pc =>
                                  Coder.transform(CQ) { qc =>
                                    Coder.transform(CR) { rc =>
                                      Coder.transform(CS)(sc =>
                                        Coder.beam(
                                          new Tuple18Coder[
                                            A,
                                            B,
                                            C,
                                            D,
                                            E,
                                            G,
                                            H,
                                            I,
                                            J,
                                            K,
                                            L,
                                            M,
                                            N,
                                            O,
                                            P,
                                            Q,
                                            R,
                                            S
                                          ](
                                            ac,
                                            bc,
                                            cc,
                                            dc,
                                            ec,
                                            gc,
                                            hc,
                                            ic,
                                            jc,
                                            kc,
                                            lc,
                                            mc,
                                            nc,
                                            oc,
                                            pc,
                                            qc,
                                            rc,
                                            sc
                                          )
                                        )
                                      )
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple19Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N],
    CO: Coder[O],
    CP: Coder[P],
    CQ: Coder[Q],
    CR: Coder[R],
    CS: Coder[S],
    CT: Coder[T]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN) { nc =>
                              Coder.transform(CO) { oc =>
                                Coder.transform(CP) { pc =>
                                  Coder.transform(CQ) { qc =>
                                    Coder.transform(CR) { rc =>
                                      Coder.transform(CS) { sc =>
                                        Coder.transform(CT)(tc =>
                                          Coder.beam(
                                            new Tuple19Coder[
                                              A,
                                              B,
                                              C,
                                              D,
                                              E,
                                              G,
                                              H,
                                              I,
                                              J,
                                              K,
                                              L,
                                              M,
                                              N,
                                              O,
                                              P,
                                              Q,
                                              R,
                                              S,
                                              T
                                            ](
                                              ac,
                                              bc,
                                              cc,
                                              dc,
                                              ec,
                                              gc,
                                              hc,
                                              ic,
                                              jc,
                                              kc,
                                              lc,
                                              mc,
                                              nc,
                                              oc,
                                              pc,
                                              qc,
                                              rc,
                                              sc,
                                              tc
                                            )
                                          )
                                        )
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple20Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N],
    CO: Coder[O],
    CP: Coder[P],
    CQ: Coder[Q],
    CR: Coder[R],
    CS: Coder[S],
    CT: Coder[T],
    CU: Coder[U]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN) { nc =>
                              Coder.transform(CO) { oc =>
                                Coder.transform(CP) { pc =>
                                  Coder.transform(CQ) { qc =>
                                    Coder.transform(CR) { rc =>
                                      Coder.transform(CS) { sc =>
                                        Coder.transform(CT) { tc =>
                                          Coder.transform(CU)(uc =>
                                            Coder.beam(
                                              new Tuple20Coder[
                                                A,
                                                B,
                                                C,
                                                D,
                                                E,
                                                G,
                                                H,
                                                I,
                                                J,
                                                K,
                                                L,
                                                M,
                                                N,
                                                O,
                                                P,
                                                Q,
                                                R,
                                                S,
                                                T,
                                                U
                                              ](
                                                ac,
                                                bc,
                                                cc,
                                                dc,
                                                ec,
                                                gc,
                                                hc,
                                                ic,
                                                jc,
                                                kc,
                                                lc,
                                                mc,
                                                nc,
                                                oc,
                                                pc,
                                                qc,
                                                rc,
                                                sc,
                                                tc,
                                                uc
                                              )
                                            )
                                          )
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple21Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N],
    CO: Coder[O],
    CP: Coder[P],
    CQ: Coder[Q],
    CR: Coder[R],
    CS: Coder[S],
    CT: Coder[T],
    CU: Coder[U],
    CV: Coder[V]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN) { nc =>
                              Coder.transform(CO) { oc =>
                                Coder.transform(CP) { pc =>
                                  Coder.transform(CQ) { qc =>
                                    Coder.transform(CR) { rc =>
                                      Coder.transform(CS) { sc =>
                                        Coder.transform(CT) { tc =>
                                          Coder.transform(CU) { uc =>
                                            Coder.transform(CV)(vc =>
                                              Coder.beam(
                                                new Tuple21Coder[
                                                  A,
                                                  B,
                                                  C,
                                                  D,
                                                  E,
                                                  G,
                                                  H,
                                                  I,
                                                  J,
                                                  K,
                                                  L,
                                                  M,
                                                  N,
                                                  O,
                                                  P,
                                                  Q,
                                                  R,
                                                  S,
                                                  T,
                                                  U,
                                                  V
                                                ](
                                                  ac,
                                                  bc,
                                                  cc,
                                                  dc,
                                                  ec,
                                                  gc,
                                                  hc,
                                                  ic,
                                                  jc,
                                                  kc,
                                                  lc,
                                                  mc,
                                                  nc,
                                                  oc,
                                                  pc,
                                                  qc,
                                                  rc,
                                                  sc,
                                                  tc,
                                                  uc,
                                                  vc
                                                )
                                              )
                                            )
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

  implicit def tuple22Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W](
    implicit
    CA: Coder[A],
    CB: Coder[B],
    CC: Coder[C],
    CD: Coder[D],
    CE: Coder[E],
    CG: Coder[G],
    CH: Coder[H],
    CI: Coder[I],
    CJ: Coder[J],
    CK: Coder[K],
    CL: Coder[L],
    CM: Coder[M],
    CN: Coder[N],
    CO: Coder[O],
    CP: Coder[P],
    CQ: Coder[Q],
    CR: Coder[R],
    CS: Coder[S],
    CT: Coder[T],
    CU: Coder[U],
    CV: Coder[V],
    CW: Coder[W]
  ): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)] =
    Coder.transform(CA) { ac =>
      Coder.transform(CB) { bc =>
        Coder.transform(CC) { cc =>
          Coder.transform(CD) { dc =>
            Coder.transform(CE) { ec =>
              Coder.transform(CG) { gc =>
                Coder.transform(CH) { hc =>
                  Coder.transform(CI) { ic =>
                    Coder.transform(CJ) { jc =>
                      Coder.transform(CK) { kc =>
                        Coder.transform(CL) { lc =>
                          Coder.transform(CM) { mc =>
                            Coder.transform(CN) { nc =>
                              Coder.transform(CO) { oc =>
                                Coder.transform(CP) { pc =>
                                  Coder.transform(CQ) { qc =>
                                    Coder.transform(CR) { rc =>
                                      Coder.transform(CS) { sc =>
                                        Coder.transform(CT) { tc =>
                                          Coder.transform(CU) { uc =>
                                            Coder.transform(CV) { vc =>
                                              Coder.transform(CW)(wc =>
                                                Coder.beam(
                                                  new Tuple22Coder[
                                                    A,
                                                    B,
                                                    C,
                                                    D,
                                                    E,
                                                    G,
                                                    H,
                                                    I,
                                                    J,
                                                    K,
                                                    L,
                                                    M,
                                                    N,
                                                    O,
                                                    P,
                                                    Q,
                                                    R,
                                                    S,
                                                    T,
                                                    U,
                                                    V,
                                                    W
                                                  ](
                                                    ac,
                                                    bc,
                                                    cc,
                                                    dc,
                                                    ec,
                                                    gc,
                                                    hc,
                                                    ic,
                                                    jc,
                                                    kc,
                                                    lc,
                                                    mc,
                                                    nc,
                                                    oc,
                                                    pc,
                                                    qc,
                                                    rc,
                                                    sc,
                                                    tc,
                                                    uc,
                                                    vc,
                                                    wc
                                                  )
                                                )
                                              )
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
}
