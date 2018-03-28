package com.spotify.scio.coders

import java.io.{IOException, ObjectInputStream, ObjectOutputStream, ObjectStreamException}
import java.util.concurrent.atomic.AtomicReference

abstract class EmptyOnSerializeAtomicReference[T] extends Serializable {

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
