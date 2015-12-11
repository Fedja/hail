package org.broadinstitute.hail.variant

import org.apache.commons.math3.distribution.BinomialDistribution
import org.scalacheck.{Gen, Arbitrary}

import scala.language.implicitConversions
import scala.collection.mutable
import org.broadinstitute.hail.Utils._

object GenotypeType extends Enumeration {
  type GenotypeType = Value
  val HomRef = Value(0)
  val Het = Value(1)
  val HomVar = Value(2)
  val NoCall = Value(-1)
}

import org.broadinstitute.hail.variant.GenotypeType.GenotypeType

// FIXME all optional...
case class Genotype(private val gt: Int,
                    ad: (Int, Int),
                    dp: Int,
                    private val pl: (Int, Int, Int),
                    fakeRef: Boolean = false) {

  require(gt >= -1 && gt <= 2)
  /*
  require(dp >= ad._1 + ad._2)
  require(gt == -1 || pl.at(gt + 1) == 0)
  require(gt != -1 || pl == null)
  */

  private def minPl: (Int, Int) = pl.remove(gt + 1)

  def write(b: mutable.ArrayBuilder[Byte]) {
    // val writeDp = ad._1 + ad._2 != dp
    val writeAd2 = gt != 0 || ad._2 != 0
    b += ((gt & 7)
//      | (if (writeDp) 0x08 else 0)
      | (if (writeAd2) 0x10 else 0)
      | (if (fakeRef) 0x20 else 0)).toByte
    if (gt != -1) {
      val (pl1, pl2) = minPl
      b.writeULEB128(pl1)
      b.writeULEB128(pl2)
    }
    b.writeULEB128(ad._1)
    if (writeAd2)
      b.writeULEB128(ad._2)
    b.writeULEB128(dp)
  }

  def isHomRef: Boolean = gt == 0
  def isHet: Boolean = gt == 1
  def isHomVar: Boolean = gt == 2
  def isCalledNonRef: Boolean = gt >= 1
  def isNotCalled: Boolean = gt == -1
  def isCalled: Boolean = gt != -1

  def gtType: GenotypeType = GenotypeType(gt)

  def gq: Int = {
    assert(gt != -1)
    val (pl1, pl2) = minPl
    pl1 min pl2 min 99
  }

  def call: Option[Call] = {
    if (gt == -1)
      None
    else
      Some(Call(gt, gq, pl))
  }

  def nNonRef: Int = if (gt == -1) 0 else gt

  override def toString: String = {
    val b = new StringBuilder
    call match {
      case Some(c) =>
        c.gt match {
          case 0 => b.append("0/0")
          case 1 => b.append("0/1")
          case 2 => b.append("1/1")
        }
      case None => b.append("./.")
    }
    b += ':'
    b.append(ad._1)
    b += ','
    b.append(ad._2)
    b += ':'
    b.append(dp)
    call match {
      case Some(c) =>
        b += ':'
        b.append(c.gq)
        b += ':'
        b.append(c.pl._1)
        b += ','
        b.append(c.pl._2)
        b += ','
        b.append(c.pl._3)
      case None =>
    }

    b.result()
  }

  def pAB(theta: Double = 0.5): Double = {
    val (refDepth, altDepth) = ad
    val d = new BinomialDistribution(refDepth + altDepth, theta)
    val minDepth = refDepth.min(altDepth)
    val minp = d.probability(minDepth)
    val mincp = d.cumulativeProbability(minDepth)
    (2 * mincp - minp).min(1.0).max(0.0)
  }
}

object Genotype {
  def read(a: Iterator[Byte]): Genotype = {
    val b = a.next()

    val gt = (b << 29) >> 29
    // val writeDp = (b & 0x08) != 0
    val writeAd2 = (b & 0x10) != 0
    val fakeRef = (b & 0x20) != 0

    val pl =
      if (gt != -1) {
        val pl1 = a.readULEB128()
        val pl2 = a.readULEB128()

        (pl1, pl2).insert(gt, 0)
      } else
        null

    val ad1: Int = a.readULEB128()
    val ad2: Int =
      if (writeAd2)
        a.readULEB128()
      else
        0

    // val dpDelta = if (writeDp) ...
    val dp = a.readULEB128()

    Genotype(gt, (ad1, ad2), dp, pl, fakeRef)
  }

  implicit def arbGenotype: Arbitrary[Genotype] = Arbitrary {
    for {gt <- Gen.choose(-1, 2)
         ad1 <- Gen.choose(0, 100)
         ad2 <- Gen.choose(0, 100)
         dpDelta <- Gen.choose(0, 10)
         pl1 <- Gen.choose(0, 1000)
         pl2 <- Gen.choose(0, 1000)
         fakeRef <- Gen.choose(0,9).map(_ == 0)}
      yield Genotype(gt, (ad1, ad2), ad1 + ad2 + dpDelta,
        if (gt == -1)
          null
        else
          (pl1, pl2).insert(gt, 0),
        fakeRef)
  }
}