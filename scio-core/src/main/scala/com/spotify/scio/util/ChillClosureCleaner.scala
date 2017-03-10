// scalastyle:off header.matches
/**
 * Copyright (c) 2010, Regents of the University of California.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the University of California, Berkeley nor the
 *       names of its contributors may be used to endorse or promote
 *       products derived from this software without specific prior written
 *       permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
// scalastyle:on header.matches

/* Ported from com.twitter.chill.ClosureCleaner */

package com.spotify.scio.util

import _root_.java.lang.reflect.Field

import org.apache.xbean.asm5.Opcodes._
import org.apache.xbean.asm5.{ClassReader, ClassVisitor, MethodVisitor, Type}

import scala.annotation.tailrec
import scala.collection.mutable.{Map => MMap, Set => MSet}

// scalastyle:off
/**
 * Copied from Spark, written by Matei Zaharia (matei@cs.berkeley.edu).
 *
 * Ported to ASM 4.0 and refactored for scalding/summingbird by Oscar Boykin
 *
 * Original code: https://github.com/mesos/spark/blob/master/core/src/main/scala/spark/ClosureCleaner.scala
 */
private[util] object ChillClosureCleaner {
  val OUTER = "$outer"

  // Here are the caches for the stuff that depends only on Class[_]
  // TODO maybe these should be thread-local for thread safety
  private val outerFields = MMap[Class[_], Option[Field]]()
  private val outerClassHier = MMap[Class[_], List[Class[_]]]()
  private val innerClasses = MMap[Class[_], Set[Class[_]]]()
  private val accessedFieldsMap = MMap[Class[_], Set[Field]]()

  private def getClassReader(cls: Class[_]): ClassReader = {
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    new ClassReader(cls.getResourceAsStream(className))
  }

  // Return the $outer field for this class
  def outerFieldOf(c: Class[_]): Option[Field] =
    outerFields
      .getOrElseUpdate(c,
        c.getDeclaredFields.find { f =>
          val scala211Outer = f.getName == OUTER
          val scala212Outer =
            f.getType.getName == c.getName.split("\\$\\$").head && f.getName.startsWith("arg$")
          scala211Outer || scala212Outer
        })

  /**
   * this does reflection each time
   * since Class objects are assumed to be immutable, we cache this result
   */
  @tailrec
  private def getOuterClassesFn(cls: Class[_], hierarchy: List[Class[_]] = Nil): List[Class[_]] =
    outerFieldOf(cls) match {
      case None => hierarchy
      case Some(f) => {
        val next = f.getType
        getOuterClassesFn(next, next :: hierarchy)
      }
    }

  def outerClassesOf(cls: Class[_]): List[Class[_]] =
    outerClassHier.getOrElseUpdate(cls, getOuterClassesFn(cls))

  /**
   * returns the (Class, AnyRef) pair from highest level to lowest level
   * so result.last is the outer of obj.
   */
  @tailrec
  def getOutersOf(obj: AnyRef, hierarchy: List[(Class[_], AnyRef)] = Nil): List[(Class[_], AnyRef)] =
    outerFieldOf(obj.getClass) match {
      case None => hierarchy // We have finished
      case Some(f) => {
        // f is the $outer of obj
        f.setAccessible(true)
        // myOuter = obj.$outer
        val myOuter = f.get(obj)
        // This is (Class[T], T) into the hierarchy:
        // Note that if you use f.getType you might get an interface. No good
        val outerType = myOuter.getClass
        getOutersOf(myOuter, (outerType, myOuter) :: hierarchy)
      }
    }

  private def getInnerClassesFn(inCls: Class[_]): Set[Class[_]] = {
    val seen = MSet[Class[_]](inCls)
    var stack = List[Class[_]](inCls)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.head)
      stack = stack.tail
      val set = MSet[Class[_]]()
      cr.accept(new InnerClosureFinder(set), 0)
      for (cls <- set -- seen) {
        seen += cls
        stack = cls :: stack
      }
    }
    (seen - inCls).toSet
  }

  def innerClassesOf(cls: Class[_]): Set[Class[_]] =
    innerClasses.getOrElseUpdate(cls, getInnerClassesFn(cls))

  private def getAccessedFields(cls: Class[_]): MMap[Class[_], MSet[String]] = {
    val af = outerClassesOf(cls)
      .foldLeft(MMap[Class[_], MSet[String]]()) { (m, clazz) =>
        m += ((clazz, MSet[String]()))
      }

    (innerClassesOf(cls) + cls).foreach { cls =>
      getClassReader(cls).accept(new FieldAccessFinder(af), 0)
    }
    af
  }

  /**
   * Uses ASM to return the names of the fields accessed by this cls
   */
  def accessedFieldsOf(cls: Class[_]): Set[Field] = {
    accessedFieldsMap.get(cls) match {
      case Some(s) => s
      case None => {
        //Compute and store:
        val af = getAccessedFields(cls)
        def toF(ss: Set[String]): Set[Field] = ss.map { cls.getDeclaredField(_) }

        val s = af.get(cls).map { _.toSet }.getOrElse(Set[String]())
        // Add all of af:
        af.foreach { clsMSet =>
          val set = clsMSet._2.toSet
          accessedFieldsMap += ((clsMSet._1, toF(set)))
        }
        toF(s)
      }
    }
  }

  def apply(obj: AnyRef): Unit = {
    val newCleanedOuter = allocCleanedOuter(obj)
    // I know the cool kids use Options, but this code
    // will avoid an allocation in the usual case of
    // no $outer
    setOuter(obj, newCleanedOuter)
  }

  /**
   * Return a new bottom-most $outer instance of this obj
   * with only the accessed fields set in the $outer parent chain
   */
  private def allocCleanedOuter(in: AnyRef): AnyRef =
    // Go top down filling in the actual accessed fields:
    getOutersOf(in)
      // the outer-most-outer is null:
      .foldLeft(null: AnyRef) { (prevOuter, clsData) =>
        val (thisOuterCls, realOuter) = clsData
        // create a new outer class that does not have the constructor
        // called on it.
        val nextOuter = instantiateClass(thisOuterCls);
        // We are populate its $outer variable with the
        // previous outer, and then we go down, and set the accessed
        // fields below:
        setOuter(nextOuter, prevOuter)
        // for each of the accessed fields of this class
        // set the fields from the parents of in:
        accessedFieldsOf(thisOuterCls)
          .foreach { setFromTo(_, realOuter, nextOuter) }
        // Now return this populated object for the next outer instance to use
        nextOuter
      }

  // Set the given field in newv to the same value as old
  private def setFromTo(f: Field, old: AnyRef, newv: AnyRef) {
    f.setAccessible(true)
    val accessedValue = f.get(old)
    f.set(newv, accessedValue)
  }

  private def setOuter(obj: AnyRef, outer: AnyRef) {
    if (null != outer) {
      val field = outerFieldOf(obj.getClass).get
      field.setAccessible(true)
      field.set(obj, outer)
    }
  }

  private val objectCtor = classOf[_root_.java.lang.Object].getDeclaredConstructor();
  // Use reflection to instantiate object without calling constructor
  private def instantiateClass(cls: Class[_]): AnyRef =
    sun.reflect.ReflectionFactory
      .getReflectionFactory
      .newConstructorForSerialization(cls, objectCtor)
      .newInstance()
      .asInstanceOf[AnyRef]
}

private class FieldAccessFinder(output: MMap[Class[_], MSet[String]]) extends ClassVisitor(ASM5) {
  override def visitMethod(access: Int, name: String, desc: String,
    sig: String, exceptions: Array[String]): MethodVisitor = {
    return new MethodVisitor(ASM5) {
      override def visitFieldInsn(op: Int, owner: String, name: String,
        desc: String) {
        if (op == GETFIELD)
          for (cl <- output.keys if cl.getName == owner.replace('/', '.'))
            output(cl) += name
      }

      override def visitMethodInsn(op: Int, owner: String, name: String,
        desc: String, itf: Boolean) {
        // Check for calls a getter method for a variable in an interpreter wrapper object.
        // This means that the corresponding field will be accessed, so we should save it.
        if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer"))
          for (cl <- output.keys if cl.getName == owner.replace('/', '.'))
            output(cl) += name
      }
    }
  }
}

private class InnerClosureFinder(output: MSet[Class[_]]) extends ClassVisitor(ASM5) {
  var myName: String = null

  override def visit(version: Int, access: Int, name: String, sig: String,
    superName: String, interfaces: Array[String]) {
    myName = name
  }

  override def visitMethod(access: Int, name: String, desc: String,
    sig: String, exceptions: Array[String]): MethodVisitor = {
    return new MethodVisitor(ASM5) {
      override def visitMethodInsn(op: Int, owner: String, name: String,
        desc: String, itf: Boolean) {
        val argTypes = Type.getArgumentTypes(desc)
        if (op == INVOKESPECIAL && name == "<init>" && argTypes.length > 0
          && argTypes(0).toString.startsWith("L") // is it an object?
          && argTypes(0).getInternalName == myName)
          output += Class.forName(owner.replace('/', '.'), false,
            Thread.currentThread.getContextClassLoader)
      }
    }
  }
}
// scalastyle:on