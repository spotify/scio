/*
Copyright 2013 Twitter, Inc.

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

package com.spotify.scio.vendor.chill

import _root_.java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  Externalizable,
  ObjectInput,
  ObjectInputStream,
  ObjectOutput,
  ObjectOutputStream
}

import _root_.java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import com.esotericsoftware.kryo.KryoSerializable

object Externalizer {
  /* Tokens used to distinguish if we used Kryo or Java */
  private val KRYO = 0
  private val JAVA = 1

  def apply[T](t: T): Externalizer[T] = {
    val x = new Externalizer[T]
    x.set(t)
    x
  }
}

/**
 * This is a more fault-tolerant MeatLocker that tries first to do Java serialization, and then
 * falls back to Kryo serialization if that does not work.
 */
class Externalizer[T] extends Externalizable with KryoSerializable {
  // Either points to a result or a delegate Externalizer to fufil that result.
  private var item: Either[Externalizer[T], Option[T]] = Right(None)
  import Externalizer._

  @transient private val doesJavaWork = new AtomicReference[Option[Boolean]](None)
  @transient private val testing = new AtomicBoolean(false)

  // No vals or var's below this line!

  def getOption: Option[T] = item match {
    case Left(e)  => e.getOption
    case Right(i) => i
  }

  def get: T = getOption.get // This should never be None when get is called

  /**
   * Unfortunately, Java serialization requires mutable objects if you are going to control how the
   * serialization is done. Use the companion object to creat new instances of this
   */
  def set(it: T): Unit =
    item match {
      case Left(e) => e.set(it)
      case Right(x) =>
        assert(x.isEmpty, "Tried to call .set on an already constructed Externalizer")
        item = Right(Some(it))
    }

  /**
   * Override this to configure Kryo creation with a named subclass, e.g. class MyExtern[T] extends
   * Externalizer[T] { override def kryo = myInstantiator } note that if this is not a named class
   * on the classpath, we have to serialize the KryoInstantiator at the same time, which would
   * increase size.
   */
  protected def kryo: KryoInstantiator =
    (new ScalaKryoInstantiator).setReferences(true)

  // 1 here is 1 thread, since we will likely only serialize once
  // this should not be a val because we don't want to capture a reference

  def javaWorks: Boolean =
    doesJavaWork.get match {
      case Some(v) => v
      case None    => probeJavaWorks
    }

  /** Try to round-trip and see if it works without error */
  private def probeJavaWorks: Boolean = {
    if (!testing.compareAndSet(false, true)) return true
    try {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(getOption)
      val bytes = baos.toByteArray
      val testInput = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(testInput)
      ois.readObject // this may throw
      doesJavaWork.set(Some(true))
      true
    } catch {
      case t: Throwable =>
        Option(System.getenv.get("CHILL_EXTERNALIZER_DEBUG"))
          .filter(_.toBoolean)
          .foreach(_ => t.printStackTrace)
        doesJavaWork.set(Some(false))
        false
    } finally {
      testing.set(false)
    }
  }

  private def safeToBytes(kryo: KryoInstantiator): Option[Array[Byte]] =
    try {
      val kpool = KryoPool.withByteArrayOutputStream(1, kryo)
      val bytes = kpool.toBytesWithClass(getOption)
      Some(bytes)
    } catch {
      case t: Throwable =>
        Option(System.getenv.get("CHILL_EXTERNALIZER_DEBUG"))
          .filter(_.toBoolean)
          .foreach(_ => t.printStackTrace)
        None
    }
  private def fromBytes(b: Array[Byte], kryo: KryoInstantiator): Option[T] =
    KryoPool
      .withByteArrayOutputStream(1, kryo)
      .fromBytes(b)
      .asInstanceOf[Option[T]]

  override def readExternal(in: ObjectInput): Unit = maybeReadJavaKryo(in, kryo)

  private def maybeReadJavaKryo(in: ObjectInput, kryo: KryoInstantiator): Unit =
    in.read match {
      case JAVA =>
        item = Right(in.readObject.asInstanceOf[Option[T]])
      case KRYO =>
        val sz = in.readInt
        val buf = new Array[Byte](sz)
        in.readFully(buf)
        item = Right(fromBytes(buf, kryo))
    }

  protected def writeJava(out: ObjectOutput): Boolean =
    javaWorks && {
      out.write(JAVA)
      out.writeObject(getOption)
      true
    }

  protected def writeKryo(out: ObjectOutput): Boolean = writeKryo(out, kryo)

  protected def writeKryo(out: ObjectOutput, kryo: KryoInstantiator): Boolean =
    safeToBytes(kryo)
      .map { bytes =>
        out.write(KRYO)
        out.writeInt(bytes.size)
        out.write(bytes)
        true
      }
      .getOrElse(false)

  private def maybeWriteJavaKryo(out: ObjectOutput, kryo: KryoInstantiator): Unit =
    writeJava(out) || writeKryo(out, kryo) || {
      val inner = get
      sys.error(
        "Neither Java nor Kryo works for class: %s instance: %s\nexport CHILL_EXTERNALIZER_DEBUG=true to see both stack traces"
          .format(inner.getClass, inner)
      )
    }

  override def writeExternal(out: ObjectOutput): Unit = maybeWriteJavaKryo(out, kryo)

  def write(kryo: Kryo, output: Output): Unit = {
    val resolver = kryo.getReferenceResolver
    resolver.getWrittenId(item) match {
      case -1 =>
        output.writeInt(-1)
        resolver.addWrittenObject(item)
        val oStream = new ObjectOutputStream(output)
        maybeWriteJavaKryo(oStream, () => kryo)
        oStream.flush
      case n =>
        output.writeInt(n)
    }
  }

  def read(kryo: Kryo, input: Input): Unit = {
    doesJavaWork.set(None)
    testing.set(false)
    val state = input.readInt()
    val resolver = kryo.getReferenceResolver
    state match {
      case -1 =>
        val objId = resolver.nextReadId(this.getClass)
        resolver.setReadObject(objId, this)
        maybeReadJavaKryo(new ObjectInputStream(input), () => kryo)
      case n =>
        val z = resolver.getReadObject(this.getClass, n).asInstanceOf[Externalizer[T]]
        if (!(z eq this)) item = Left(z)
    }
  }
}
