package org.broadinstitute.hail.methods

import org.broadinstitute.hail.Utils
import org.broadinstitute.hail.variant.{Sample, Genotype, Variant}
import scala.math
import scala.reflect.ClassTag
import scala.language.implicitConversions

class FilterString(val s: String) extends AnyVal {
  def ~(t: String): Boolean = s.r.findFirstIn(t).isDefined
  def !~(t: String): Boolean = !this.~(t)
}

class FilterOption[T](val ot: Option[T]) extends AnyVal {
  //override def toString: Option[String] = ot.map(_.toString)
  def ==(ot2: Option[T]): Option[Boolean] = ot.flatMap(t => ot2.map(_ == t))
  def !=(ot2: Option[T]): Option[Boolean] = ot.flatMap(t => ot2.map(_ != t))
}

class FilterBooleanOption(val ob: Option[Boolean]) extends AnyVal {
  def &&(ob2: Option[Boolean]): Option[Boolean] = ob.flatMap(b => ob2.map(b && _))
  def ||(ob2: Option[Boolean]): Option[Boolean] = ob.flatMap(b => ob2.map(b || _))
  def unary_!(): Option[Boolean] = ob.map(! _)
}

class FilterStringOption(val os: Option[String]) extends AnyVal {
  def toInt: Option[Int] = os.map(_.toInt)
  def toDouble: Option[Double] = os.map(_.toDouble)
  def +(os2: Option[String]) = os.flatMap(s => os2.map(s + _))
  def apply(i: Int) = os.map(_(i))
}

class FilterArrayOption[T](val oa: Option[Array[T]]) extends AnyVal {
  def apply(i: Int): Option[T] = oa.map(_(i))
}

class FilterOrderedOption[T <: Ordered[T]](val ot: Option[T]) extends AnyVal {
  def >(ot2: Option[T]): Option[Boolean] = ot.flatMap(t => ot2.map(t > _))
}

class FilterNumericOption[T <: scala.math.Numeric[T]#Ops](val ot: Option[T]) extends AnyVal {
  def +(ot2: Option[T]): Option[T] = ot.flatMap(t => ot2.map(t + _))
  def -(ot2: Option[T]): Option[T] = ot.flatMap(t => ot2.map(t - _))
  def *(ot2: Option[T]): Option[T] = ot.flatMap(t => ot2.map(t * _))
  def unary_-(): Option[T] = ot.map(-_)
  def abs: Option[T] = ot.map(_.abs())
  def signum: Option[Int] = ot.map(_.signum())
  def toInt: Option[Int] = ot.map(_.toInt())
  def toLong: Option[Long] = ot.map(_.toLong())
  def toFloat: Option[Float] = ot.map(_.toFloat())
  def toDouble: Option[Double] = ot.map(_.toDouble())


//  def /(rhs: FilterNumericOption[T]) =
//    if (o.isDefined && rhs.o.isDefined && rhs.o.get != 0) new FilterNumericOption[T](Some(o.get / rhs.o.get))
//    else new FilterNumericOption[T](None)

}

object FilterUtils {

  implicit def toFilterString(s: String): FilterString = new FilterString(s)
  implicit def toFilterOption[T](t: T): FilterOption[T] = new FilterOption[T](Some(t))
  implicit def toFilterStringOption(s: String): FilterStringOption = new FilterStringOption(Some(s))
  implicit def toFilterBooleanOption(b: Boolean): FilterBooleanOption = new FilterBooleanOption(Some(b))
  implicit def toFilterArrayOption[T](a: Array[T]): FilterArrayOption[T] = new FilterArrayOption[T](Some(a))
  implicit def toFilterOrderedOption[T <: Ordered[T]](t: T): FilterOrderedOption[T] = new FilterOrderedOption[T](Some(t))
  implicit def toFilterNumericOption[T <: scala.math.Numeric[T]#Ops](t: T): FilterNumericOption[T] = new FilterNumericOption[T](Some(t))



}

class Evaluator[T](t: String)(implicit tct: ClassTag[T])
  extends Serializable {
  @transient var p: Option[T] = None

  def typeCheck() {
    require(p.isEmpty)
    p = Some(Utils.eval[T](t))
  }

  def eval(): T = p match {
    case null | None =>
      val v = Utils.eval[T](t)
      p = Some(v)
      v
    case Some(v) => v
  }
}

class FilterVariantCondition(cond: String)
  extends Evaluator[(Variant) => Boolean](
    "(v: org.broadinstitute.hail.variant.Variant) => { " +
      "import org.broadinstitute.hail.methods.FilterUtils._; " +
      cond + " }: Boolean") {
  def apply(v: Variant): Boolean = eval()(v)
}

class FilterSampleCondition(cond: String)
  extends Evaluator[(Sample) => Boolean](
    "(s: org.broadinstitute.hail.variant.Sample) => { " +
      "import org.broadinstitute.hail.methods.FilterUtils._; " +
      cond + " }: Boolean") {
  def apply(s: Sample): Boolean = eval()(s)
}

class FilterGenotypeCondition(cond: String)
  extends Evaluator[(Variant, Sample, Genotype) => Boolean](
    "(v: org.broadinstitute.hail.variant.Variant, " +
      "s: org.broadinstitute.hail.variant.Sample, " +
      "g: org.broadinstitute.hail.variant.Genotype) => { " +
      "import org.broadinstitute.hail.methods.FilterUtils._; " +
      cond + " }: Boolean") {
  def apply(v: Variant, s: Sample, g: Genotype): Boolean =
    eval()(v, s, g)
}
