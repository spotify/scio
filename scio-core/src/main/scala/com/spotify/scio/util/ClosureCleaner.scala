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

package com.spotify.scio.util

import java.io.NotSerializableException
import java.lang.reflect.Field

import org.apache.beam.sdk.util.SerializableUtils
import org.apache.xbean.asm6.Opcodes._
import org.apache.xbean.asm6.{ClassReader, ClassVisitor, MethodVisitor, Type}

import scala.annotation.tailrec
import scala.collection.mutable.{Map => MMap, Set => MSet, Stack => MStack}
import scala.language.existentials
import scala.util.Try

private[scio] object ClosureCleaner {
  private[this] val OUTER = "$outer"

  /** Clean the closure in place. */
  final def apply[T <: AnyRef](func: T): T = {
    try {
      SerializableUtils.serializeToByteArray(func.asInstanceOf[Serializable])
    } catch {
      case e: IllegalArgumentException if e.getCause.isInstanceOf[NotSerializableException] =>
        clean(func)
      case _: ClassCastException =>
        clean(func)
    }
    func
  }

  private def clean(func: AnyRef): AnyRef =
    new TransitiveClosureCleaner(func).clean

  def outerFieldOf(c: Class[_]): Option[Field] =
    Try(c.getDeclaredField(OUTER)).toOption

  def isOuterField(f: Field): Boolean = f.getName == OUTER

  /**
   * Returns the (Class, AnyRef) pairs from highest level to lowest level. The last element is the
   * outer of the closure.
   */
  def outerClassesOf(obj: AnyRef): List[(Class[_], AnyRef)] = {
    @tailrec
    def loop(obj: AnyRef, hierarchy: List[(Class[_], AnyRef)]): List[(Class[_], AnyRef)] = {
      outerFieldOf(obj.getClass) match {
        case None    => hierarchy // We have finished
        case Some(f) =>
          // f is the $outer of obj
          f.setAccessible(true)
          // myOuter = obj.$outer
          val myOuter = f.get(obj)
          val outerType = myOuter.getClass

          loop(myOuter, (outerType, myOuter) :: hierarchy)
      }
    }

    loop(obj, Nil)
  }

  def innerClassesOf(func: AnyRef): Set[Class[_]] = {
    val seen = MSet[Class[_]](func.getClass)
    val stack = MStack[Class[_]](func.getClass)
    while (stack.nonEmpty) {
      val cr = AsmUtil.classReader(stack.pop())
      val set = MSet[Class[_]]()
      cr.foreach { reader =>
        reader.accept(new InnerClosureFinder(set), 0)
        (set -- seen).foreach { cls =>
          seen += cls
          stack.push(cls)
        }
      }
    }
    (seen - func.getClass).toSet
  }

  def setOuter(obj: AnyRef, outer: AnyRef): Unit =
    if (outer != null) {
      val field = outerFieldOf(obj.getClass).get
      field.setAccessible(true)
      field.set(obj, outer)
    }

  def copyField(f: Field, old: AnyRef, newv: AnyRef): Unit = {
    f.setAccessible(true)
    val accessedValue = f.get(old)
    f.set(newv, accessedValue)
  }

  def instantiateClass(cls: Class[_]): AnyRef = {
    val objectCtor = classOf[java.lang.Object].getDeclaredConstructor()

    sun.reflect.ReflectionFactory.getReflectionFactory
      .newConstructorForSerialization(cls, objectCtor)
      .newInstance()
      .asInstanceOf[AnyRef]
  }
}

private trait ClosureCleaner {
  import ClosureCleaner._

  /** The closure to clean. */
  val func: AnyRef

  /** Clean [[func]] by replacing its 'outer' with a cleaned clone. See [[cleanOuter()]]. */
  def clean: AnyRef = {
    val newOuter = cleanOuter()
    setOuter(func, newOuter)
    func
  }

  /**
   * Create a new 'cleaned' copy of [[func]]'s outer, without modifying the original. The cleaned
   * outer may have null values for fields that are determined to be unneeded in the context
   * of [[func]].
   *
   * @return The cleaned outer.
   */
  def cleanOuter(): AnyRef
}

/**
 * An implementation of [[ClosureCleaner]] that cleans [[func]] by transitively tracing its method
 * calls and field references up through its enclosing scopes. A new hierarchy of outers is
 * constructed by cloning the outer scopes and populating only the fields that are to be accessed
 * by [[func]] (including any of its inner closures). Additionally, outers are removed from the
 * new hierarchy if none of their fields are accessed by [[func]].
 */
private final class TransitiveClosureCleaner(val func: AnyRef) extends ClosureCleaner {
  import ClosureCleaner._

  private lazy val outerClasses: List[(Class[_], AnyRef)] = outerClassesOf(func)
  private lazy val accessedFieldsMap: Map[Class[_], Set[Field]] = {
    val accessedFields = outerClasses
      .map(_._1)
      .foldLeft(MMap[Class[_], MSet[String]]()) { (m, cls) =>
        m += ((cls, MSet[String]()))
      }

    (innerClassesOf(func) + func.getClass)
      .foreach {
        AsmUtil.classReader(_).foreach { reader =>
          reader.accept(new AccessedFieldsVisitor(accessedFields), 0)
        }
      }

    accessedFields.map {
      case (cls, mset) =>
        def toF(ss: Set[String]): Set[Field] = ss.map(cls.getDeclaredField)
        val set = mset.toSet
        (cls, toF(set))
    }.toMap
  }

  override def cleanOuter(): AnyRef =
    outerClasses.foldLeft(null: AnyRef) { (prevOuter, clsData) =>
      val (thisOuterCls, realOuter) = clsData
      val nextOuter = instantiateClass(thisOuterCls)
      accessedFieldsMap(thisOuterCls).foreach(copyField(_, realOuter, nextOuter))
      /* If this object's outer is not transitively referenced from the starting closure
         (or any of its inner closures), we can null it out. */
      val parent = if (!accessedFieldsMap(thisOuterCls).exists(isOuterField)) {
        null
      } else {
        prevOuter
      }
      setOuter(nextOuter, parent)
      nextOuter
    }
}

private final case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

private final class AccessedFieldsVisitor(output: MMap[Class[_], MSet[String]],
                                          specificMethod: Option[MethodIdentifier[_]] = None,
                                          visitedMethods: MSet[MethodIdentifier[_]] = MSet.empty)
    extends ClassVisitor(ASM6) {
  override def visitMethod(access: Int,
                           name: String,
                           desc: String,
                           sig: String,
                           exceptions: Array[String]): MethodVisitor = {
    if (specificMethod.isDefined &&
        (specificMethod.get.name != name || specificMethod.get.desc != desc)) {
      null
    } else {
      new MethodVisitor(ASM6) {
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String): Unit = {
          if (op == GETFIELD) {
            for (cl <- output.keys if cl.getName == owner.replace('/', '.')) {
              output(cl) += name
            }
          }
        }

        override def visitMethodInsn(op: Int,
                                     owner: String,
                                     name: String,
                                     desc: String,
                                     itf: Boolean): Unit = {
          for (cl <- output.keys if cl.getName == owner.replace('/', '.')) {
            // Check for calls a getter method for a variable in an interpreter wrapper object.
            // This means that the corresponding field will be accessed, so we should save it.
            if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
              output(cl) += name
            }
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              // Keep track of visited methods to avoid potential infinite cycles
              visitedMethods += m
              AsmUtil.classReader(cl).foreach { reader =>
                reader.accept(new AccessedFieldsVisitor(output, Some(m), visitedMethods), 0)
              }
            }
          }
        }
      }
    }
  }
}

private final class InnerClosureFinder(output: MSet[Class[_]]) extends ClassVisitor(ASM6) {
  private[this] var myName: String = _

  override def visit(version: Int,
                     access: Int,
                     name: String,
                     sig: String,
                     superName: String,
                     interfaces: Array[String]): Unit =
    myName = name

  override def visitMethod(access: Int,
                           name: String,
                           desc: String,
                           sig: String,
                           exceptions: Array[String]): MethodVisitor =
    new MethodVisitor(ASM6) {
      override def visitMethodInsn(op: Int,
                                   owner: String,
                                   name: String,
                                   desc: String,
                                   itf: Boolean): Unit = {
        val argTypes = Type.getArgumentTypes(desc)
        if (op == INVOKESPECIAL && name == "<init>" && argTypes.nonEmpty
            && argTypes(0).toString.startsWith("L")
            && argTypes(0).getInternalName == myName) {
          output += Class.forName(owner.replace('/', '.'),
                                  false,
                                  Thread.currentThread.getContextClassLoader)
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
