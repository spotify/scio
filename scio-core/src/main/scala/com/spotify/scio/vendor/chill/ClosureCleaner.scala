/**
 * Copyright (c) 2010, Regents of the University of California. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of California, Berkeley nor the names of its contributors
 * may be used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.spotify.scio.vendor.chill

import _root_.java.lang.reflect.Field
import _root_.java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}
import org.apache.xbean.asm7.Opcodes._
import org.apache.xbean.asm7.{ClassReader, ClassVisitor, MethodVisitor, Type}

import scala.annotation.tailrec
import scala.collection.mutable.{Map => MMap, Set => MSet, Stack => MStack}
import scala.util.Try

/**
 * Copied from Spark, written by Matei Zaharia (matei@cs.berkeley.edu).
 *
 * Ported to ASM 4.0 and refactored for scalding/summingbird by Oscar Boykin
 *
 * Original code:
 * https://github.com/mesos/spark/blob/master/core/src/main/scala/spark/ClosureCleaner.scala
 */
object ClosureCleaner {
  val OUTER = "$outer"

  // Here are the caches for the stuff that depends only on Class[_]
  // TODO maybe these should be thread-local for thread safety
  private val outerFields = MMap[Class[_], Option[Field]]()
  private val outerClassHier = MMap[Class[_], List[Class[_]]]()
  private val innerClasses = MMap[Class[_], Set[Class[_]]]()
  private val accessedFieldsMap = MMap[Class[_], Set[Field]]()

  private[chill] def serialize[T](t: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    try {
      out.writeObject(t)
      bos.toByteArray
    } finally {
      out.close()
      bos.close()
    }
  }

  private[chill] def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val in = new ObjectInputStream(bis);
    try {
      in.readObject.asInstanceOf[T]
    } finally {
      bis.close()
      in.close()
    }
  }

  // Return the $outer field for this class
  def outerFieldOf(c: Class[_]): Option[Field] =
    outerFields
      .getOrElseUpdate(
        c,
        c.getDeclaredFields.find {
          _.getName == OUTER
        }
      )

  /**
   * this does reflection each time since Class objects are assumed to be immutable, we cache this
   * result
   */
  @tailrec
  private def getOuterClassesFn(cls: Class[_], hierarchy: List[Class[_]] = Nil): List[Class[_]] =
    outerFieldOf(cls) match {
      case None => hierarchy
      case Some(f) =>
        val next = f.getType
        getOuterClassesFn(next, next :: hierarchy)
    }

  def outerClassesOf(cls: Class[_]): List[Class[_]] =
    outerClassHier.getOrElseUpdate(cls, getOuterClassesFn(cls))

  /**
   * returns the (Class, AnyRef) pair from highest level to lowest level so result.last is the outer
   * of obj.
   */
  @tailrec
  def getOutersOf(
    obj: AnyRef,
    hierarchy: List[(Class[_], AnyRef)] = Nil
  ): List[(Class[_], AnyRef)] =
    outerFieldOf(obj.getClass) match {
      case None    => hierarchy // We have finished
      case Some(f) =>
        // f is the $outer of obj
        f.setAccessible(true)
        // myOuter = obj.$outer
        val myOuter = f.get(obj)
        // This is (Class[T], T) into the hierarchy:
        // Note that if you use f.getType you might get an interface. No good
        val outerType = myOuter.getClass
        getOutersOf(myOuter, (outerType, myOuter) :: hierarchy)
    }

  private def getInnerClassesFn(inCls: Class[_]): Set[Class[_]] = {
    val seen = MSet[Class[_]](inCls)
    val stack = MStack[Class[_]](inCls)
    while (stack.nonEmpty) {
      // mutated by InnerClosureFinder
      val set = MSet[Class[_]]()
      AsmUtil.classReader(stack.pop()).foreach(cr => cr.accept(new InnerClosureFinder(set), 0))
      (set -- seen).foreach { cls =>
        seen += cls
        stack.push(cls)
      }
    }
    (seen - inCls).toSet
  }

  def innerClassesOf(cls: Class[_]): Set[Class[_]] =
    innerClasses.getOrElseUpdate(cls, getInnerClassesFn(cls))

  private def getAccessedFields(cls: Class[_]): Map[Class[_], Set[Field]] = {
    val accessedFields = outerClassesOf(cls)
      .foldLeft(MMap[Class[_], MSet[String]]())((m, cls) => m += ((cls, MSet[String]())))

    (innerClassesOf(cls) + cls).foreach {
      AsmUtil.classReader(_).foreach(cr => cr.accept(new FieldAccessFinder(accessedFields), 0))
    }

    accessedFields.iterator.map { case (cls, mset) =>
      def toF(ss: Set[String]): Set[Field] = ss.map(cls.getDeclaredField)
      val set = mset.toSet
      (cls, toF(set))
    }.toMap
  }

  /** Uses ASM to return the names of the fields accessed by this cls */
  def accessedFieldsOf(cls: Class[_]): Set[Field] =
    accessedFieldsMap.get(cls) match {
      case Some(s) => s
      case None    =>
        // Compute and store:
        val af = getAccessedFields(cls)
        // Add all of af:
        accessedFieldsMap ++= af
        af.getOrElse(cls, Set.empty)
    }

  def clean[T <: AnyRef](obj: T): T =
    Try {
      deserialize[T](serialize(obj.asInstanceOf[Serializable]))
    }.getOrElse {
      val newCleanedOuter = allocCleanedOuter(obj)
      // I know the cool kids use Options, but this code
      // will avoid an allocation in the usual case of
      // no $outer
      setOuter(obj, newCleanedOuter)
      obj
    }

  def apply(obj: AnyRef): Unit = clean(obj)

  def isOuterField(f: Field): Boolean = f.getName == OUTER

  /**
   * Return a new bottom-most $outer instance of this obj with only the accessed fields set in the
   * $outer parent chain
   */
  private def allocCleanedOuter(in: AnyRef): AnyRef = {
    // loads accessed fieds for $in
    accessedFieldsOf(in.getClass)
    // Go top down filling in the actual accessed fields:
    getOutersOf(in)
      // the outer-most-outer is null:
      .foldLeft(null: AnyRef) { (prevOuter, clsData) =>
        val (thisOuterCls, realOuter) = clsData
        // create a new outer class that does not have the constructor
        // called on it.
        val nextOuter = instantiateClass(thisOuterCls)

        val af = accessedFieldsOf(thisOuterCls)
        // for each of the accessed fields of this class
        // set the fields from the parents of in:
        af.foreach(setFromTo(_, realOuter, nextOuter))
        // If this object's outer is not transitively referenced from the starting closure
        // (or any of its inner closures), we can null it out.
        val parent = af.find(isOuterField).map(_ => prevOuter).orNull
        // We are populate its $outer variable with the
        // previous outer, and then we go down, and set the accessed
        // fields below:
        setOuter(nextOuter, parent)
        // Now return this populated object for the next outer instance to use
        nextOuter
      }
  }

  // Set the given field in newv to the same value as old
  private def setFromTo(f: Field, old: AnyRef, newv: AnyRef): Unit = {
    f.setAccessible(true)
    val accessedValue = f.get(old)
    f.set(newv, accessedValue)
  }

  private def setOuter(obj: AnyRef, outer: AnyRef): Unit =
    if (null != outer) {
      outerFieldOf(obj.getClass).foreach { field =>
        field.setAccessible(true)
        field.set(obj, outer)
      }
    }

  // Use reflection to instantiate object without calling constructor
  def instantiateClass(cls: Class[_]): AnyRef = {
    val objectCtor = classOf[_root_.java.lang.Object].getDeclaredConstructor()

    sun.reflect.ReflectionFactory.getReflectionFactory
      .newConstructorForSerialization(cls, objectCtor)
      .newInstance()
      .asInstanceOf[AnyRef]
  }
}

case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

class FieldAccessFinder(
  output: MMap[Class[_], MSet[String]],
  specificMethod: Option[MethodIdentifier[_]] = None,
  visitedMethods: MSet[MethodIdentifier[_]] = MSet.empty
) extends ClassVisitor(ASM7) {
  override def visitMethod(
    access: Int,
    name: String,
    desc: String,
    sig: String,
    exceptions: Array[String]
  ): MethodVisitor =
    if (
      specificMethod.isDefined &&
      (specificMethod.get.name != name || specificMethod.get.desc != desc)
    ) {
      null
    } else {
      new MethodVisitor(ASM7) {
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String): Unit =
          if (op == GETFIELD) {
            val ownerName = owner.replace('/', '.')
            output.keys.iterator
              .filter(_.getName == ownerName)
              .foreach(cl => output(cl) += name)
          }

        override def visitMethodInsn(
          op: Int,
          owner: String,
          name: String,
          desc: String,
          itf: Boolean
        ): Unit = {
          val ownerName = owner.replace('/', '.')
          output.keys.iterator.filter(_.getName == ownerName).foreach { cl =>
            // Check for calls a getter method for a variable in an interpreter wrapper object.
            // This means that the corresponding field will be accessed, so we should save it.
            if (
              op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith(
                "$outer"
              )
            ) {
              output(cl) += name
            }
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              // Keep track of visited methods to avoid potential infinite cycles
              visitedMethods += m
              AsmUtil.classReader(cl).foreach { cr =>
                cr.accept(new FieldAccessFinder(output, Some(m), visitedMethods), 0)
              }
            }
          }
        }
      }
    }
}

/**
 * Find inner closures and avoid class initialization
 *
 * {{{
 *   val closure1 = (i: Int) => {
 *     Option(i).map { x =>
 *       x + someSerializableValue // inner closure
 *     }
 *   }
 * }}}
 */
class InnerClosureFinder(output: MSet[Class[_]]) extends ClassVisitor(ASM7) {
  var myName: String = _

  override def visit(
    version: Int,
    access: Int,
    name: String,
    sig: String,
    superName: String,
    interfaces: Array[String]
  ): Unit =
    myName = name

  override def visitMethod(
    access: Int,
    name: String,
    desc: String,
    sig: String,
    exceptions: Array[String]
  ): MethodVisitor =
    new MethodVisitor(ASM7) {
      override def visitMethodInsn(
        op: Int,
        owner: String,
        name: String,
        desc: String,
        itf: Boolean
      ): Unit = {
        val argTypes = Type.getArgumentTypes(desc)
        if (
          op == INVOKESPECIAL && name == "<init>" && argTypes.nonEmpty
          && argTypes(0).toString.startsWith("L")
          && argTypes(0).getInternalName == myName
        ) {
          output += Class.forName(
            owner.replace('/', '.'),
            false,
            Thread.currentThread.getContextClassLoader
          )
        }
      }
    }
}

private object AsmUtil {
  def classReader(cls: Class[_]): Option[ClassReader] = {
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    Try(new ClassReader(cls.getResourceAsStream(className))).toOption
  }
}
