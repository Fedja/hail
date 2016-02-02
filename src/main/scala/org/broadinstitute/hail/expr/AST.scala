package org.broadinstitute.hail.expr

import org.broadinstitute.hail.annotations.Annotations
import org.broadinstitute.hail.variant.{Variant, Genotype}
import scala.util.parsing.input.{Position, Positional}

object Type {
  val sampleFields = Map(
    "id" -> TString)

  val genotypeFields = Map(
    "gt" -> TInt,
    "ad" -> TArray(TInt),
    "dp" -> TInt,
    "gq" -> TInt,
    "pl" -> TArray(TInt))

  val variantFields = Map(
    "chrom" -> TString,
    "start" -> TInt,
    "ref" -> TString,
    "alt" -> TString)
}

trait NumericConversion[T] extends Serializable {
  def to(numeric: Any): T
}

object IntNumericConversion extends NumericConversion[Int] {
  def to(numeric: Any): Int = numeric match {
    case i: Int => i
  }
}

object LongNumericConversion extends NumericConversion[Long] {
  def to(numeric: Any): Long = numeric match {
    case i: Int => i
    case l: Long => l
  }
}

object FloatNumericConversion extends NumericConversion[Float] {
  def to(numeric: Any): Float = numeric match {
    case i: Int => i
    case l: Long => l
    case f: Float => f
  }
}

object DoubleNumericConversion extends NumericConversion[Double] {
  def to(numeric: Any): Double = numeric match {
    case i: Int => i
    case l: Long => l
    case f: Float => f
    case d: Double => d
  }
}

sealed abstract class Type extends Serializable

case object TBoolean extends Type {
  override def toString = "Boolean"
}

case object TChar extends Type {
  override def toString = "Char"
}

abstract class TNumeric extends Type

case object TInt extends TNumeric {
  override def toString = "Int"
}

case object TLong extends TNumeric {
  override def toString = "Long"
}

case object TFloat extends TNumeric {
  override def toString = "Float"
}

case object TDouble extends TNumeric {
  override def toString = "Double"
}

case object TUnit extends Type {
  override def toString = "Unit"
}

case object TString extends Type {
  override def toString = "String"
}

case class TArray(elementType: Type) extends Type {
  override def toString = s"Array[$elementType]"
}

case class TSet(elementType: Type) extends Type {
  override def toString = s"Set[$elementType]"
}

case class TFunction(parameterTypes: Array[Type], returnType: Type) extends Type {
  override def toString = s"(${parameterTypes.mkString(",")}) => $returnType"
}

// FIXME name?
class TAbstractStruct(fields: Map[String, Type]) extends Type

case object TSample extends TAbstractStruct(Type.sampleFields) {
  override def toString = "Sample"
}

case object TGenotype extends TAbstractStruct(Type.genotypeFields) {
  override def toString = "Genotype"
}

case object TVariant extends TAbstractStruct(Type.variantFields) {
  override def toString = "Variant"
}

case class TStruct(fields: Map[String, Type]) extends TAbstractStruct(fields) {
  override def toString = "Struct"
}

object AST extends Positional with Serializable {
  def promoteNumeric(t: TNumeric): Type = t

  def promoteNumeric(lhs: TNumeric, rhs: TNumeric): Type =
    if (lhs == TDouble || rhs == TDouble)
      TDouble
    else if (lhs == TFloat || rhs == TFloat)
      TFloat
    else if (lhs == TLong || rhs == TLong)
      TLong
    else
      TInt

  def evalFlatCompose[T](c: EvalContext, subexpr: AST)
    (g: (T) => Any): () => Any = {
    val f = subexpr.eval(c)
    () => {
      val x = f()
      if (x != null)
        g(x.asInstanceOf[T])
      else
        null
    }
  }

  def evalCompose[T](c: EvalContext, subexpr: AST)
    (g: (T) => Any): () => Any = {
    val f = subexpr.eval(c)
    () => {
      val x = f()
      if (x != null)
        g(x.asInstanceOf[T])
      else
        null
    }
  }

  def evalCompose[T1, T2](c: EvalContext, subexpr1: AST, subexpr2: AST)
    (g: (T1, T2) => Any): () => Any = {
    val f1 = subexpr1.eval(c)
    val f2 = subexpr2.eval(c)
    () => {
      val x = f1()
      if (x != null) {
        val y = f2()
        if (y != null)
          g(x.asInstanceOf[T1], y.asInstanceOf[T2])
        else
          null
      } else
        null
    }
  }

  def evalCompose[T1, T2, T3](c: EvalContext, subexpr1: AST, subexpr2: AST, subexpr3: AST)
    (g: (T1, T2, T3) => Any): () => Any = {
    val f1 = subexpr1.eval(c)
    val f2 = subexpr2.eval(c)
    val f3 = subexpr3.eval(c)
    () => {
      val x = f1()
      if (x != null) {
        val y = f2()
        if (y != null) {
          val z = f3()
          if (z != null)
            g(x.asInstanceOf[T1], y.asInstanceOf[T2], z.asInstanceOf[T3])
          else
            null
        } else
          null
      } else
        null
    }
  }

  def evalComposeNumeric[T](c: EvalContext, subexpr: AST)
    (g: (T) => Any)
    (implicit convT: NumericConversion[T]): () => Any = {
    val f = subexpr.eval(c)
    () => {
      val x = f()
      if (x != null)
        g(convT.to(x))
      else
        null
    }
  }


  def evalComposeNumeric[T1, T2](c: EvalContext, subexpr1: AST, subexpr2: AST)
    (g: (T1, T2) => Any)
    (implicit convT1: NumericConversion[T1], convT2: NumericConversion[T2]): () => Any = {
    val f1 = subexpr1.eval(c)
    val f2 = subexpr2.eval(c)
    () => {
      val x = f1()
      if (x != null) {
        val y = f2()
        if (y != null)
          g(convT1.to(x), convT2.to(y))
        else
          null
      } else
        null
    }
  }
}

case class Positioned[T](x: T) extends Positional

sealed abstract class AST(pos: Position, subexprs: Array[AST] = Array.empty) extends Serializable {
  var `type`: Type = null

  def this(posn: Position, subexpr1: AST) = this(posn, Array(subexpr1))

  def this(posn: Position, subexpr1: AST, subexpr2: AST) = this(posn, Array(subexpr1, subexpr2))

  def eval(c: EvalContext): () => Any

  def typecheckThis(typeSymTab: SymbolTable): Type = typecheckThis()

  def typecheckThis(): Type = throw new UnsupportedOperationException

  def typecheck(typeSymTab: SymbolTable) {
    subexprs.foreach(_.typecheck(typeSymTab))
    `type` = typecheckThis(typeSymTab)
  }

  def parseError(msg: String): Nothing = ParserUtils.error(pos, msg)
}

case class Const(posn: Position, value: Any, t: Type) extends AST(posn) {
  def eval(c: EvalContext): () => Any = {
    val v = value
    () => v
  }

  override def typecheckThis(): Type = t
}

case class Select(posn: Position, lhs: AST, rhs: String) extends AST(posn, lhs) {
  override def typecheckThis(): Type = {
    (lhs.`type`, rhs) match {
      // Sample
      case (TSample, "id") => TString
      // Genotype
      case (TGenotype, "gt") => TInt
      case (TGenotype, "ad") => TArray(TInt)
      case (TGenotype, "dp") => TInt
      case (TGenotype, "gq") => TInt
      case (TGenotype, "pl") => TArray(TInt)
      case (TGenotype, "isHomRef") => TBoolean
      case (TGenotype, "isHet") => TBoolean
      case (TGenotype, "isHomVar") => TBoolean
      case (TGenotype, "isCalledNonRef") => TBoolean
      case (TGenotype, "isCalled") => TBoolean
      case (TGenotype, "isNotCalled") => TBoolean
      case (TGenotype, "nNonRef") => TInt
      case (TGenotype, "pAB") => TFunction(Array(), TDouble)
      // Variant
      case (TVariant, "contig") => TString
      case (TVariant, "start") => TInt
      case (TVariant, "ref") => TString
      case (TVariant, "alt") => TString
      case (TVariant, "wasSplit") => TBoolean
      case (TVariant, "inParX") => TBoolean
      case (TVariant, "inParY") => TBoolean
      case (TVariant, "isSNP") => TBoolean
      case (TVariant, "isMNP") => TBoolean
      case (TVariant, "isIndel") => TBoolean
      case (TVariant, "isInsertion") => TBoolean
      case (TVariant, "isDeletion") => TBoolean
      case (TVariant, "isComplex") => TBoolean
      case (TVariant, "isTransition") => TBoolean
      case (TVariant, "isTransversion") => TBoolean
      case (TVariant, "nMismatch") => TInt
      case (t@TStruct(fields), _) => {
        fields.get(rhs) match {
          case Some(t) => t
          case None => parseError(s"`$t' has no field `$rhs'")
        }
      }
      case (t: TNumeric, "toInt") => TInt
      case (t: TNumeric, "toLong") => TLong
      case (t: TNumeric, "toFloat") => TFloat
      case (t: TNumeric, "toDouble") => TDouble
      case (t: TNumeric, "abs") => t
      case (t: TNumeric, "signum") => TInt
      case (t: TNumeric, "min") => TFunction(Array(t), t)
      case (t: TNumeric, "max") => TFunction(Array(t), t)
      case (TString, "length") => TInt
      case (TString, "split") => TFunction(Array(TString), TArray(TString))
      case (TArray(TString), "mkString") => TFunction(Array(TString), TString)
      case (TArray(_), "length") => TInt
      case (TArray(_), "isEmpty") => TBoolean
      case (TArray(elementType), "contains") => TFunction(Array(elementType), TBoolean)
      case (TSet(_), "size") => TInt
      case (TSet(_), "isEmpty") => TBoolean
      case (TSet(elementType), "contains") => TFunction(Array(elementType), TBoolean)
      case (_, "isMissing") => TBoolean
      case (_, "isNotMissing") => TBoolean

      case (t, _) =>
        parseError(s"`$t' has no field `$rhs'")
    }
  }

  def eval(c: EvalContext): () => Any = ((lhs.`type`, rhs): @unchecked) match {
    case (TSample, "id") => lhs.eval(c)

    case (TGenotype, "gt") =>
      AST.evalFlatCompose[Genotype](c, lhs)(_.call.map(_.gt))
    case (TGenotype, "ad") =>
      AST.evalCompose[Genotype](c, lhs)(g => Array[Int](g.ad._1, g.ad._2): IndexedSeq[Int])
    case (TGenotype, "dp") =>
      AST.evalCompose[Genotype](c, lhs)(_.dp)
    case (TGenotype, "gq") =>
      AST.evalCompose[Genotype](c, lhs)(_.gq)
    case (TGenotype, "pl") =>
      AST.evalFlatCompose[Genotype](c, lhs)(_.call.map(_.pl))
    case (TGenotype, "isHomRef") =>
      AST.evalCompose[Genotype](c, lhs)(_.isHomRef)
    case (TGenotype, "isHet") =>
      AST.evalCompose[Genotype](c, lhs)(_.isHet)
    case (TGenotype, "isHomVar") =>
      AST.evalFlatCompose[Genotype](c, lhs)(_.isHomVar)
    case (TGenotype, "isCalledNonRef") =>
      AST.evalFlatCompose[Genotype](c, lhs)(_.isCalledNonRef)
    case (TGenotype, "isCalled") =>
      AST.evalFlatCompose[Genotype](c, lhs)(_.isCalled)
    case (TGenotype, "isNotCalled") =>
      AST.evalFlatCompose[Genotype](c, lhs)(_.isNotCalled)
    case (TGenotype, "nNonRef") =>
      AST.evalFlatCompose[Genotype](c, lhs)(_.nNonRef)
    case (TGenotype, "pAB") =>
      AST.evalFlatCompose[Genotype](c, lhs)(_.pAB())

    case (TVariant, "contig") =>
      AST.evalCompose[Variant](c, lhs)(_.contig)
    case (TVariant, "start") =>
      AST.evalCompose[Variant](c, lhs)(_.start)
    case (TVariant, "ref") =>
      AST.evalCompose[Variant](c, lhs)(_.ref)
    case (TVariant, "alt") =>
      AST.evalCompose[Variant](c, lhs)(_.alt)
    case (TVariant, "wasSplit") =>
      AST.evalCompose[Variant](c, lhs)(_.wasSplit)
    case (TVariant, "inParX") =>
      AST.evalCompose[Variant](c, lhs)(_.inParX)
    case (TVariant, "inParY") =>
      AST.evalCompose[Variant](c, lhs)(_.inParY)
    case (TVariant, "isSNP") =>
      AST.evalCompose[Variant](c, lhs)(_.isSNP)
    case (TVariant, "isMNP") =>
      AST.evalCompose[Variant](c, lhs)(_.isMNP)
    case (TVariant, "isIndel") =>
      AST.evalCompose[Variant](c, lhs)(_.isIndel)
    case (TVariant, "isInsertion") =>
      AST.evalCompose[Variant](c, lhs)(_.isInsertion)
    case (TVariant, "isDeletion") =>
      AST.evalCompose[Variant](c, lhs)(_.isDeletion)
    case (TVariant, "isComplex") =>
      AST.evalCompose[Variant](c, lhs)(_.isComplex)
    case (TVariant, "isTransition") =>
      AST.evalCompose[Variant](c, lhs)(_.isTransition)
    case (TVariant, "isTransversion") =>
      AST.evalCompose[Variant](c, lhs)(_.isTransversion)
    case (TVariant, "nMismatch") =>
      AST.evalCompose[Variant](c, lhs)(_.nMismatch)

    case (TStruct(fields), _) =>
      AST.evalFlatCompose[Map[String, Any]](c, lhs) { m =>
        val x = m.getOrElse(rhs, null)
        if (x != null
          && x.isInstanceOf[Annotations])
          x.asInstanceOf[Annotations].attrs
        else
          x
      }

    case (TInt, "toInt") => lhs.eval(c)
    case (TInt, "toLong") => AST.evalCompose[Int](c, lhs)(_.toLong)
    case (TInt, "toFloat") => AST.evalCompose[Int](c, lhs)(_.toFloat)
    case (TInt, "toDouble") => AST.evalCompose[Int](c, lhs)(_.toDouble)

    case (TLong, "toInt") => AST.evalCompose[Long](c, lhs)(_.toInt)
    case (TLong, "toLong") => lhs.eval(c)
    case (TLong, "toFloat") => AST.evalCompose[Long](c, lhs)(_.toFloat)
    case (TLong, "toDouble") => AST.evalCompose[Long](c, lhs)(_.toDouble)

    case (TFloat, "toInt") => AST.evalCompose[Float](c, lhs)(_.toInt)
    case (TFloat, "toLong") => AST.evalCompose[Float](c, lhs)(_.toLong)
    case (TFloat, "toFloat") => lhs.eval(c)
    case (TFloat, "toDouble") => AST.evalCompose[Float](c, lhs)(_.toDouble)

    case (TDouble, "toInt") => AST.evalCompose[Double](c, lhs)(_.toInt)
    case (TDouble, "toLong") => AST.evalCompose[Double](c, lhs)(_.toLong)
    case (TDouble, "toFloat") => AST.evalCompose[Double](c, lhs)(_.toFloat)
    case (TDouble, "toDouble") => lhs.eval(c)

    case (TString, "toInt") => AST.evalCompose[String](c, lhs)(_.toInt)
    case (TString, "toLong") => AST.evalCompose[String](c, lhs)(_.toLong)
    case (TString, "toFloat") => AST.evalCompose[String](c, lhs)(_.toFloat)
    case (TString, "toDouble") => AST.evalCompose[String](c, lhs)(_.toDouble)

    case (_, "isMissing") => AST.evalCompose[Any](c, lhs)(_ == null)
    case (_, "isNotMissing") => AST.evalCompose[Any](c, lhs)(_ != null)

    case (TInt, "abs") => AST.evalCompose[Int](c, lhs)(_.abs)
    case (TLong, "abs") => AST.evalCompose[Long](c, lhs)(_.abs)
    case (TFloat, "abs") => AST.evalCompose[Float](c, lhs)(_.abs)
    case (TDouble, "abs") => AST.evalCompose[Double](c, lhs)(_.abs)

    case (TInt, "signum") => AST.evalCompose[Int](c, lhs)(_.signum)
    case (TLong, "signum") => AST.evalCompose[Long](c, lhs)(_.signum)
    case (TFloat, "signum") => AST.evalCompose[Float](c, lhs)(_.signum)
    case (TDouble, "signum") => AST.evalCompose[Double](c, lhs)(_.signum)

    case (TInt, "max") => AST.evalCompose[Int](c, lhs)(a => (b: Int) => a.max(b))
    case (TInt, "min") => AST.evalCompose[Int](c, lhs)(a => (b: Int) => a.min(b))

    case (TLong, "max") => AST.evalCompose[Long](c, lhs)(a => (b: Long) => a.max(b))
    case (TLong, "min") => AST.evalCompose[Long](c, lhs)(a => (b: Long) => a.min(b))

    case (TFloat, "max") => AST.evalCompose[Float](c, lhs)(a => (b: Float) => a.max(b))
    case (TFloat, "min") => AST.evalCompose[Float](c, lhs)(a => (b: Float) => a.min(b))

    case (TDouble, "max") => AST.evalCompose[Double](c, lhs)(a => (b: Double) => a.max(b))
    case (TDouble, "min") => AST.evalCompose[Double](c, lhs)(a => (b: Double) => a.min(b))

    case (TString, "length") => AST.evalCompose[String](c, lhs)(_.length)
    case (TString, "split") => AST.evalCompose[String](c, lhs)(s => (d: String) => s.split(d): IndexedSeq[String])
    case (TString, "mkString") => AST.evalCompose[String](c, lhs)(s => (d: String) => s.split(d): IndexedSeq[String])

    case (TArray(_), "length") => AST.evalCompose[IndexedSeq[_]](c, lhs)(_.length)
    case (TArray(_), "isEmpty") => AST.evalCompose[IndexedSeq[_]](c, lhs)(_.isEmpty)
    case (TArray(_), "contains") => AST.evalCompose[IndexedSeq[Any]](c, lhs)(s => (x: Any) => s.contains(x))

    case (TSet(_), "size") => AST.evalCompose[Set[_]](c, lhs)(_.size)
    case (TSet(_), "isEmpty") => AST.evalCompose[Set[_]](c, lhs)(_.isEmpty)
    case (TSet(_), "contains") => AST.evalCompose[Set[Any]](c, lhs)(s => (x: Any) => s.contains(x))
  }
}

case class BinaryOp(posn: Position, lhs: AST, operation: String, rhs: AST) extends AST(posn, lhs, rhs) {
  def eval(c: EvalContext): () => Any = ((operation, `type`): @unchecked) match {
    case ("+", TString) => AST.evalCompose[String, String](c, lhs, rhs)(_ + _)
    case ("~", TBoolean) => AST.evalCompose[String, String](c, lhs, rhs) { (s, t) =>
      s.r.findFirstIn(t).isDefined
    }

    case ("||", TBoolean) => {
      val f1 = lhs.eval(c)
      val f2 = rhs.eval(c)

      () => {
        val x = f1()
        if (x != null) {
          if (x.asInstanceOf[Boolean])
            true
          else
            f2()
        } else
          null
      }
    }

    case ("&&", TBoolean) => {
      val f1 = lhs.eval(c)
      val f2 = rhs.eval(c)
      () => {
        val x = f1()
        if (x != null) {
          if (x.asInstanceOf[Boolean])
            f2()
          else
            false
        } else
          null
      }
    }

    case ("+", TInt) => AST.evalComposeNumeric[Int, Int](c, lhs, rhs)(_ + _)
    case ("-", TInt) => AST.evalComposeNumeric[Int, Int](c, lhs, rhs)(_ - _)
    case ("*", TInt) => AST.evalComposeNumeric[Int, Int](c, lhs, rhs)(_ * _)
    case ("/", TInt) => AST.evalComposeNumeric[Int, Int](c, lhs, rhs)(_ / _)
    case ("%", TInt) => AST.evalComposeNumeric[Int, Int](c, lhs, rhs)(_ % _)

    case ("+", TDouble) => AST.evalComposeNumeric[Double, Double](c, lhs, rhs)(_ + _)
    case ("-", TDouble) => AST.evalComposeNumeric[Double, Double](c, lhs, rhs)(_ - _)
    case ("*", TDouble) => AST.evalComposeNumeric[Double, Double](c, lhs, rhs)(_ * _)
    case ("/", TDouble) => AST.evalComposeNumeric[Double, Double](c, lhs, rhs)(_ / _)

    case ("+", TSet(_)) => AST.evalCompose[Set[Any], Any](c, lhs, rhs)(_ + _)
    case ("-", TSet(_)) => AST.evalCompose[Set[Any], Any](c, lhs, rhs)(_ - _)
  }

  override def typecheckThis(): Type = (lhs.`type`, operation, rhs.`type`) match {
    case (TString, "+", TString) => TString
    case (TString, "~", TString) => TBoolean
    case (TBoolean, "||", TBoolean) => TBoolean
    case (TBoolean, "&&", TBoolean) => TBoolean
    case (lhsType: TNumeric, "+", rhsType: TNumeric) => AST.promoteNumeric(lhsType, rhsType)
    case (lhsType: TNumeric, "-", rhsType: TNumeric) => AST.promoteNumeric(lhsType, rhsType)
    case (lhsType: TNumeric, "*", rhsType: TNumeric) => AST.promoteNumeric(lhsType, rhsType)
    case (lhsType: TNumeric, "/", rhsType: TNumeric) => AST.promoteNumeric(lhsType, rhsType)

    case (lhsType, _, rhsType) =>
      parseError(s"invalid arguments to `$operation': ($lhsType, $rhsType)")
  }
}

case class Comparison(posn: Position, lhs: AST, operation: String, rhs: AST) extends AST(posn, lhs, rhs) {
  var operandType: Type = null

  def eval(c: EvalContext): () => Any = ((operation, operandType): @unchecked) match {
    case ("==", _) => AST.evalCompose[Any, Any](c, lhs, rhs)(_ == _)
    case ("!=", _) => AST.evalCompose[Any, Any](c, lhs, rhs)(_ != _)

    case ("<", TInt) => AST.evalComposeNumeric[Int, Int](c, lhs, rhs)(_ < _)
    case ("<=", TInt) => AST.evalComposeNumeric[Int, Int](c, lhs, rhs)(_ <= _)
    case (">", TInt) => AST.evalComposeNumeric[Int, Int](c, lhs, rhs)(_ > _)
    case (">=", TInt) => AST.evalComposeNumeric[Int, Int](c, lhs, rhs)(_ >= _)

    case ("<", TLong) => AST.evalComposeNumeric[Long, Long](c, lhs, rhs)(_ < _)
    case ("<=", TLong) => AST.evalComposeNumeric[Long, Long](c, lhs, rhs)(_ <= _)
    case (">", TLong) => AST.evalComposeNumeric[Long, Long](c, lhs, rhs)(_ > _)
    case (">=", TLong) => AST.evalComposeNumeric[Long, Long](c, lhs, rhs)(_ >= _)

    case ("<", TFloat) => AST.evalComposeNumeric[Float, Float](c, lhs, rhs)(_ < _)
    case ("<=", TFloat) => AST.evalComposeNumeric[Float, Float](c, lhs, rhs)(_ <= _)
    case (">", TFloat) => AST.evalComposeNumeric[Float, Float](c, lhs, rhs)(_ > _)
    case (">=", TFloat) => AST.evalComposeNumeric[Float, Float](c, lhs, rhs)(_ >= _)

    case ("<", TDouble) => AST.evalComposeNumeric[Double, Double](c, lhs, rhs)(_ < _)
    case ("<=", TDouble) => AST.evalComposeNumeric[Double, Double](c, lhs, rhs)(_ <= _)
    case (">", TDouble) => AST.evalComposeNumeric[Double, Double](c, lhs, rhs)(_ > _)
    case (">=", TDouble) => AST.evalComposeNumeric[Double, Double](c, lhs, rhs)(_ >= _)
  }

  override def typecheckThis(): Type = {
    operandType = (lhs.`type`, operation, rhs.`type`) match {
      case (_, "==" | "!=", _) => null
      case (lhsType: TNumeric, "<=" | ">=" | "<" | ">", rhsType: TNumeric) =>
        AST.promoteNumeric(lhsType, rhsType)

      case (lhsType, _, rhsType) =>
        parseError(s"invalid arguments to `$operation': ($lhsType, $rhsType)")
    }

    TBoolean
  }
}

case class UnaryOp(posn: Position, operation: String, operand: AST) extends AST(posn, operand) {
  def eval(c: EvalContext): () => Any = ((operation, `type`): @unchecked) match {
    case ("-", TInt) => AST.evalComposeNumeric[Int](c, operand)(-_)
    case ("-", TLong) => AST.evalComposeNumeric[Long](c, operand)(-_)
    case ("-", TFloat) => AST.evalComposeNumeric[Float](c, operand)(-_)
    case ("-", TDouble) => AST.evalComposeNumeric[Double](c, operand)(-_)

    case ("!", TBoolean) => AST.evalCompose[Boolean](c, operand)(!_)
  }

  override def typecheckThis(): Type = (operation, operand.`type`) match {
    case ("-", t: TNumeric) => AST.promoteNumeric(t)
    case ("!", TBoolean) => TBoolean

    case (_, t) =>
      parseError(s"invalid argument to unary `$operation': ${t.toString}")
  }
}

case class Apply(posn: Position, f: AST, args: Array[AST]) extends AST(posn, f +: args) {
  override def typecheckThis(): Type = (f.`type`, args.map(_.`type`)) match {
    case (TArray(elementType), Array(TInt)) => elementType

    case (TFunction(parameterTypes, returnType), argumentTypes) => {
      if (parameterTypes.length != argumentTypes.length)
        parseError("wrong number of arguments in application")

      for (i <- parameterTypes.indices) {
        val p = parameterTypes(i)
        val a = argumentTypes(i)

        if (p != a)
          args(i).parseError(s"argument ${i + 1} invalid type: expected `$p', got `$a'")
      }

      returnType
    }

    case _ =>
      parseError("invalid arguments to application")
  }

  def eval(c: EvalContext): () => Any = ((f.`type`, args.map(_.`type`)): @unchecked) match {
    case (TArray(elementType), Array(TInt)) =>
      AST.evalCompose[IndexedSeq[_], Int](c, f, args(0))((a, i) => a(i))

    case (TString, Array(TInt)) =>
      AST.evalCompose[String, Int](c, f, args(0))((s, i) => s(i))

    case (TFunction(Array(paramType), returnType), Array(argType)) =>
      AST.evalCompose[(Any) => Any, Any](c, f, args(0))((f, a) => f(a))
    case (TFunction(Array(param1Type, param2Type), returnType), Array(arg1Type, arg2Type)) =>
      AST.evalCompose[(Any, Any) => Any, Any, Any](c, f, args(0), args(1))((f, a1, a2) => f(a1, a2))

  }

}

case class SymRef(posn: Position, symbol: String) extends AST(posn) {
  def eval(c: EvalContext): () => Any = {
    val i = c._1(symbol)._1
    val a = c._2
    () => a(i)
  }

  override def typecheckThis(typeSymTab: SymbolTable): Type = typeSymTab.get(symbol) match {
    case Some((_, t)) => t
    case None =>
      parseError(s"symbol `$symbol' not found")
  }
}

case class If(pos: Position, cond: AST, thenTree: AST, elseTree: AST)
  extends AST(pos, Array(cond, thenTree, elseTree)) {
  override def typecheckThis(typeSymTab: SymbolTable): Type =
    TBoolean

  def eval(c: EvalContext): () => Any = {
    val f1 = cond.eval(c)
    val f2 = thenTree.eval(c)
    val f3 = elseTree.eval(c)
    () => {
      val c = f1()
      if (c != null) {
        if (c.asInstanceOf[Boolean])
          f2()
        else
          f3()
      } else
        null
    }
  }
}
