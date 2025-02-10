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

package com.spotify.scio.vendor.chill

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.reflectasm.ConstructorAccess
import com.esotericsoftware.kryo.serializers.FieldSerializer

import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.strategy.InstantiatorStrategy
import _root_.java.lang.reflect.{Constructor, Modifier}

import com.esotericsoftware.kryo.ClassResolver
import com.esotericsoftware.kryo.ReferenceResolver
import com.esotericsoftware.kryo.util.DefaultClassResolver
import com.esotericsoftware.kryo.util.MapReferenceResolver

import scala.util.{Failure, Success, Try}

/*
 * This is the base class of Kryo we use to fix specific scala
 * related issues discovered (ideally, this should be fixed in Kryo)
 */
class KryoBase(classResolver: ClassResolver, referenceResolver: ReferenceResolver)
    extends Kryo(classResolver, referenceResolver) {
  // no-args constructor instead of constructor with default args for binary compatibility
  def this() =
    this(new DefaultClassResolver, new MapReferenceResolver)

  lazy val objSer = new ObjectSerializer[AnyRef]

  protected var strategy: Option[InstantiatorStrategy] = None

  val functions: Iterable[Class[_]] = List(
    classOf[Function0[_]],
    classOf[Function1[_, _]],
    classOf[Function2[_, _, _]],
    classOf[Function3[_, _, _, _]],
    classOf[Function4[_, _, _, _, _]],
    classOf[Function5[_, _, _, _, _, _]],
    classOf[Function6[_, _, _, _, _, _, _]],
    classOf[Function7[_, _, _, _, _, _, _, _]],
    classOf[Function8[_, _, _, _, _, _, _, _, _]],
    classOf[Function9[_, _, _, _, _, _, _, _, _, _]],
    classOf[Function10[_, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function11[_, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function12[_, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function13[_, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]],
    classOf[Function22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]]
  )

  def isFn(klass: Class[_]): Boolean =
    functions.find(_.isAssignableFrom(klass)).isDefined

  override def newDefaultSerializer(klass: Class[_]): KSerializer[_] =
    if (isSingleton(klass)) {
      objSer
    } else {
      super.newDefaultSerializer(klass) match {
        case fs: FieldSerializer[_] =>
          // Scala has a lot of synthetic fields that must be serialized:
          // We also enable it by default in java since not wanting these fields
          // serialized looks like the exception rather than the rule.
          fs.setIgnoreSyntheticFields(false)

          /**
           * This breaks scalding, but something like this should be used when working with the
           * repl.
           *
           * if(isFn(klass)) new CleaningSerializer(fs.asInstanceOf[FieldSerializer[AnyRef]]) else
           */
          fs
        case x: KSerializer[_] => x
      }
    }

  /** return true if this class is a scala "object" */
  def isSingleton(klass: Class[_]): Boolean =
    klass.getName.last == '$' && objSer.accepts(klass)

  // Get the strategy if it is not null
  def tryStrategy(cls: Class[_]): InstantiatorStrategy =
    strategy.getOrElse {
      val name = cls.getName
      if (cls.isMemberClass() && !Modifier.isStatic(cls.getModifiers()))
        throw new KryoException("Class cannot be created (non-static member class): " + name);
      else
        throw new KryoException("Class cannot be created (missing no-arg constructor): " + name);
    }

  override def setInstantiatorStrategy(st: InstantiatorStrategy): Unit = {
    super.setInstantiatorStrategy(st)
    strategy = Some(st)
  }

  /* Fixes the case where Kryo's reflectasm doesn't work, even though it claims to
   * TODO this should be fixed in Kryo. When it is, remove this
   */
  override def newInstantiator(cls: Class[_]): ObjectInstantiator[AnyRef] =
    newTypedInstantiator[AnyRef](cls.asInstanceOf[Class[AnyRef]])

  private[this] def newTypedInstantiator[T](cls: Class[T]) = {
    import Instantiators._
    newOrElse(
      cls,
      List(reflectAsm[T](_), normalJava[T](_)),
      // Or fall back on the strategy:
      tryStrategy(cls).newInstantiatorOf(cls)
    )
  }
}

object Instantiators {
  // Go through the list and use the first that works
  def newOrElse[T](
    cls: Class[T],
    it: TraversableOnce[Class[T] => Try[ObjectInstantiator[T]]],
    elsefn: => ObjectInstantiator[T]
  ): ObjectInstantiator[T] =
    // Just go through and try each one,
    it.flatMap(fn => fn(cls).toOption)
      .find(_ => true) // first element in traversable once (no headOption defined.)
      .getOrElse(elsefn)

  // Use call by name:
  def forClass[T](t: Class[T])(fn: () => T): ObjectInstantiator[T] =
    new ObjectInstantiator[T] {
      override def newInstance(): T =
        try {
          fn()
        } catch {
          case x: Exception =>
            throw new KryoException("Error constructing instance of class: " + t.getName, x)
        }
    }

  // This one tries reflectasm, which is a fast way of constructing an object
  def reflectAsm[T](t: Class[T]): Try[ObjectInstantiator[T]] =
    try {
      val access = ConstructorAccess.get(t)
      // Try it once, because this isn't always successful:
      access.newInstance
      // Okay, looks good:
      Success(forClass(t)(() => access.newInstance()))
    } catch {
      case x: Throwable => Failure(x)
    }

  def getConstructor[T](c: Class[T]): Constructor[T] =
    try {
      c.getConstructor()
    } catch {
      case _: Throwable =>
        val cons = c.getDeclaredConstructor()
        cons.setAccessible(true)
        cons
    }

  def normalJava[T](t: Class[T]): Try[ObjectInstantiator[T]] =
    try {
      val cons = getConstructor(t)
      Success(forClass(t)(() => cons.newInstance()))
    } catch {
      case x: Throwable => Failure(x)
    }
}
