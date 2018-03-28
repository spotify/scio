package com.spotify.scio.coders

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.serializers.FieldSerializer
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.instantiator.sun.SunReflectionFactoryInstantiator

import scala.reflect.ClassTag

class CaseClassSerializer[T: ClassTag](kryo: Kryo) extends FieldSerializer[T](kryo, classOf[T]) {

  val objectCreator: ObjectInstantiator[T] = new SunReflectionFactoryInstantiator[T](classOf[T])

  override def create(kryo: Kryo, input: Input, classType: Class[T]): T = {
    objectCreator.newInstance()
  }

}