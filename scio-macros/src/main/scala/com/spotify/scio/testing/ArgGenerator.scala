package com.spotify.scio.testing
import caseapp.core.util.CaseUtil

sealed trait Arg {
  type Type

  def name: Option[String]

  def value: Type
}

final case class IntArg(name: Option[String], value: Int) extends Arg {
  override type Type = Int
}

final case class LongArg(name: Option[String], value: Long) extends Arg {
  override type Type = Long
}

final case class StringArg(name: Option[String], value: String) extends Arg {
  override type Type = String
}

final case class BooleanArg(name: Option[String], value: Boolean) extends Arg {
  override type Type = Boolean
}

trait ArgGenerator[T] {
  def gen(name: Option[String], t: T): List[Arg]

  def gen(name: String, t: T): List[Arg] = gen(Some(name), t)

  def gen(t: T): List[Arg] = gen(None, t)
}

object ArgGenerator
    extends LowPriorityArgGeneratorImplicits
    with LowPriorityArgGeneratorDerivation {

  @inline final def apply[T: ArgGenerator]: ArgGenerator[T] = implicitly[ArgGenerator[T]]

}

sealed trait LowPriorityArgGeneratorImplicits {
  implicit val int: ArgGenerator[Int] = new ArgGenerator[Int] {
    override def gen(name: Option[String], t: Int): List[Arg] = IntArg(name, t) :: Nil
  }

  implicit val long: ArgGenerator[Long] = new ArgGenerator[Long] {
    override def gen(name: Option[String], t: Long): List[Arg] = LongArg(name, t) :: Nil
  }

  implicit val string: ArgGenerator[String] = new ArgGenerator[String] {
    override def gen(name: Option[String], t: String): List[Arg] = StringArg(name, t) :: Nil
  }

  implicit val boolean: ArgGenerator[Boolean] = new ArgGenerator[Boolean] {
    override def gen(name: Option[String], t: Boolean): List[Arg] = BooleanArg(name, t) :: Nil
  }

  implicit def list[T](implicit ArgGenerator: ArgGenerator[T]): ArgGenerator[List[T]] =
    new ArgGenerator[List[T]] {
      override def gen(name: Option[String], t: List[T]): List[Arg] =
        t.flatMap(e => ArgGenerator.gen(name, e))
    }
}

trait LowPriorityArgGeneratorDerivation {
  import scala.language.experimental.macros, magnolia._

  type Typeclass[T] = ArgGenerator[T]

  def combine[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = new Typeclass[T] {
    override def gen(name: Option[String], t: T): List[Arg] = {
      caseClass.parameters.foldLeft(List[Arg]()) { (acc, p) =>
        val flagName = name.map(n => s"$n-${p.label}").getOrElse(p.label)
        acc ++ p.typeclass.gen(flagName, p.dereference(t))
      }
    }
  }

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]
}

trait ArgFormatter[T <: Arg] {
  def format(t: T)(implicit stringCaseFormatter: StringCaseFormatter): List[String]
}

object ArgFormatter extends LowPriorityArgFormatterImplicits {
  @inline final def apply[A <: Arg](implicit s: ArgFormatter[A]): ArgFormatter[A] = s

  def format[A <: Arg](arg: A)(implicit af: ArgFormatter[A],
                               stringCaseFormatter: StringCaseFormatter =
                                 StringCaseFormatter.pascalCase): List[String] =
    ArgFormatter[A].format(arg)
}

sealed trait LowPriorityArgFormatterImplicits {
  def formatter[A <: Arg](f: A#Type => String): ArgFormatter[A] = new ArgFormatter[A] {
    override def format(t: A)(implicit scf: StringCaseFormatter): List[String] =
      t.name.map(scf).map(n => s"--$n=${f(t.value)}").getOrElse(f(t.value)) :: Nil
  }

  implicit val string: ArgFormatter[StringArg] = formatter[StringArg](identity)
  implicit val int: ArgFormatter[IntArg] = formatter(_.toString)
  implicit val long: ArgFormatter[LongArg] = formatter(_.toString)
  implicit val boolean: ArgFormatter[BooleanArg] = formatter(_.toString)

  implicit val arg: ArgFormatter[Arg] = new ArgFormatter[Arg] {
    override def format(t: Arg)(implicit scf: StringCaseFormatter): List[String] = t match {
      case a: IntArg    => ArgFormatter.format(a)
      case a: LongArg   => ArgFormatter.format(a)
      case a: StringArg => ArgFormatter.format(a)
      case a: BooleanArg => ArgFormatter.format(a)
    }
  }
}

trait StringCaseFormatter extends (String => String) {}

object StringCaseFormatter {
  def pascalCase: StringCaseFormatter = new StringCaseFormatter {
    override def apply(str: String): String =
      CaseUtil.pascalCaseSplit(str.toList).map(_.toLowerCase).mkString("-")
  }
}
