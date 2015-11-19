package org.broadinstitute.hail.variant

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.variant.vsm.{SparkyVSM, TupleVSM}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object VariantSampleMatrix {
  def apply(vsmtype: String,
            metadata: VariantMetadata,
            rdd: RDD[(Variant, GenotypeStream)]): VariantSampleMatrix[Genotype] = {
    vsmtype match {
      case "sparky" => new SparkyVSM(metadata, rdd)
      case "tuple" => TupleVSM(metadata, rdd)
    }
  }

  def read(sqlContext: SQLContext, dirname: String) = {
    val (vsmType, metadata) = readObjectFile(dirname + "/metadata.ser", sqlContext.sparkContext.hadoopConfiguration)(
      _.readObject().asInstanceOf[(String, VariantMetadata)])

    vsmType match {
      case "sparky" => SparkyVSM.read(sqlContext, dirname, metadata)
      case "tuple" => TupleVSM.read(sqlContext, dirname, metadata)
    }
  }
}

// FIXME all maps should become RDDs
abstract class VariantSampleMatrix[T](val metadata: VariantMetadata,
  val localSamples: Array[Int]) {

  def sampleIds: Array[String] = metadata.sampleIds
  def nSamples: Int = metadata.sampleIds.length
  def nLocalSamples: Int = localSamples.length

  def sparkContext: SparkContext

  // underlying RDD
  def nPartitions: Int
  def cache(): VariantSampleMatrix[T]
  def repartition(nPartitions: Int): VariantSampleMatrix[T]

  def variants: RDD[Variant]
  def nVariants: Long = variants.count()

  def expand(): RDD[(Variant, Int, T)]

  def write(sqlContext: SQLContext, dirname: String)

  def mapValuesWithKeys[U](f: (Variant, Int, T) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): VariantSampleMatrix[U]

  def mapValues[U](f: (T) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): VariantSampleMatrix[U] = {
    mapValuesWithKeys((v, s, g) => f(g))
  }

  def mapWithKeys[U](f: (Variant, Int, T) => U)(implicit uct: ClassTag[U]): RDD[U]
  def map[U](f: T => U)(implicit uct: ClassTag[U]): RDD[U] =
    mapWithKeys((v, s, g) => f(g))

  def flatMapWithKeys[U](f: (Variant, Int, T) => TraversableOnce[U])(implicit uct: ClassTag[U]): RDD[U]
  def flatMap[U](f: T => TraversableOnce[U])(implicit uct: ClassTag[U]): RDD[U] =
    flatMapWithKeys((v, s, g) => f(g))

  def filterVariants(p: (Variant) => Boolean): VariantSampleMatrix[T]
  def filterVariants(ilist: IntervalList): VariantSampleMatrix[T] =
    filterVariants(v => ilist.contains(v.contig, v.start))

  def filterSamples(p: (Int) => Boolean): VariantSampleMatrix[T]

  def aggregateBySampleWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, Int, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Int, U)]

  def aggregateBySample[U](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Int, U)] =
    aggregateBySampleWithKeys(zeroValue)((e, v, s, g) => seqOp(e, g), combOp)

  def aggregateByVariantWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, Int, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Variant, U)]

  def aggregateByVariant[U](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Variant, U)] =
    aggregateByVariantWithKeys(zeroValue)((e, v, s, g) => seqOp(e, g), combOp)

  def foldBySample(zeroValue: T)(combOp: (T, T) => T): RDD[(Int, T)]

  def foldByVariant(zeroValue: T)(combOp: (T, T) => T): RDD[(Variant, T)]

  def fullOuterJoin[S](other: VariantSampleMatrix[S]): VariantSampleMatrix[(Option[T],Option[S])]

  def leftOuterJoin[S](other: VariantSampleMatrix[S]): VariantSampleMatrix[(T,Option[S])]

  def rightOuterJoin[S](other: VariantSampleMatrix[S]): VariantSampleMatrix[(Option[T],S)]

  def innerJoin[S](other: VariantSampleMatrix[S]): VariantSampleMatrix[(T,S)]

}
