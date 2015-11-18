package org.broadinstitute.hail.variant.vsm

import java.nio.ByteBuffer
import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.variant._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object SparkyVSM {
  def read(sqlContext: SQLContext, dirname: String, metadata: VariantMetadata): SparkyVSM[Genotype, GenotypeStream] = {
    import RichRow._

    require(dirname.endsWith(".vds"))

    // val df = sqlContext.read.parquet(dirname + "/rdd.parquet")
    val df = sqlContext.parquetFile(dirname + "/rdd.parquet")
    new SparkyVSM[Genotype, GenotypeStream](metadata, df.rdd.map(r => (r.getVariant(0), r.getGenotypeStream(1))))
  }
}

class SparkyVSM[T, S <: Iterable[T]](metadata: VariantMetadata,
  localSamples: Array[Int],
  val rdd: RDD[(Variant, S)])
  (implicit ttt: TypeTag[T], stt: TypeTag[S], tct: ClassTag[T], sct: ClassTag[S],
    vct: ClassTag[Variant])
  extends VariantSampleMatrix[T](metadata, localSamples) {

  def this(metadata: VariantMetadata, rdd: RDD[(Variant, S)])
    (implicit ttt: TypeTag[T], stt: TypeTag[S], tct: ClassTag[T], sct: ClassTag[S]) =
    this(metadata, Array.range(0, metadata.nSamples), rdd)

  def copy[U, V <: Iterable[U]](metadata: VariantMetadata = this.metadata,
    localSamples: Array[Int] = this.localSamples,
    rdd: RDD[(Variant, V)] = this.rdd)
    (implicit ttt: TypeTag[U], stt: TypeTag[V], tct: ClassTag[U], sct: ClassTag[V]): SparkyVSM[U, V] =
    new SparkyVSM[U, V](metadata, localSamples, rdd)

  def sparkContext: SparkContext = rdd.sparkContext

  def cache() = copy(rdd = rdd.cache())

  def repartition(nPartitions: Int) = copy(rdd = rdd.repartition(nPartitions))

  def nPartitions: Int = rdd.partitions.length

  def variants: RDD[Variant] = rdd.keys

  def expand(): RDD[(Variant, Int, T)] =
    mapWithKeys[(Variant, Int, T)]((v, s, g) => (v, s, g))

  def write(sqlContext: SQLContext, dirname: String) {
    import sqlContext.implicits._

    require(dirname.endsWith(".vds"))

    val hConf = sparkContext.hadoopConfiguration
    hadoopMkdir(dirname, hConf)
    writeObjectFile(dirname + "/metadata.ser", hConf)(
      _.writeObject("sparky" -> metadata))

    // rdd.toDF().write.parquet(dirname + "/rdd.parquet")
    rdd.toDF().saveAsParquetFile(dirname + "/rdd.parquet")
  }

  def mapValuesWithKeys[U](f: (Variant, Int, T) => U)
    (implicit utt: TypeTag[U], uct: ClassTag[U]): SparkyVSM[U, Iterable[U]] = {
    val localSamplesBc = sparkContext.broadcast(localSamples)
    copy(rdd = rdd.map { case (v, gs) =>
      (v, localSamplesBc.value.view.zip(gs.view)
        .map { case (s, t) => f(v, s, t) })
    })
  }

  def mapWithKeys[U](f: (Variant, Int, T) => U)(implicit uct: ClassTag[U]): RDD[U] = {
    val localSamplesBc = sparkContext.broadcast(localSamples)
    rdd
      .flatMap { case (v, gs) => localSamplesBc.value.view.zip(gs.view)
        .map { case (s, g) => f(v, s, g) }
      }
  }

  def flatMapWithKeys[U](f: (Variant, Int, T) => TraversableOnce[U])(implicit uct: ClassTag[U]): RDD[U] = {
    val localSamplesBc = sparkContext.broadcast(localSamples)
    rdd
      .flatMap { case (v, gs) => localSamplesBc.value.view.zip(gs.view)
        .flatMap { case (s, g) => f(v, s, g) }
      }
  }

  def filterVariants(p: (Variant) => Boolean) =
    copy(rdd = rdd.filter { case (v, _) => p(v) })

  def filterSamples(p: (Int) => Boolean) = {
    val localSamplesBc = sparkContext.broadcast(localSamples)
    copy(localSamples = localSamples.filter(p),
      rdd = rdd.map { case (v, gs) =>
        (v, localSamplesBc.value.view.zip(gs.view)
          .filter { case (s, _) => p(s) }
          .map(_._2))
      })
  }

  def aggregateBySampleWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, Int, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Int, U)] = {

    val localSamplesBc = sparkContext.broadcast(localSamples)

    val serializer = SparkEnv.get.serializer.newInstance()
    val zeroBuffer = serializer.serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    rdd
      .mapPartitions { (it: Iterator[(Variant, S)]) =>
        val serializer = SparkEnv.get.serializer.newInstance()
        def copyZeroValue() = serializer.deserialize[U](ByteBuffer.wrap(zeroArray))
        val arrayZeroValue = Array.fill[U](localSamplesBc.value.length)(copyZeroValue())

        localSamplesBc.value.iterator
          .zip(it.foldLeft(arrayZeroValue) { case (acc, (v, gs)) =>
            for ((g, i) <- gs.zipWithIndex)
              acc(i) = seqOp(acc(i), v, localSamplesBc.value(i), g)
            acc
          }.iterator)
      }.foldByKey(zeroValue)(combOp)
  }

  def aggregateByVariantWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, Int, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Variant, U)] = {

    val localSamplesBc = sparkContext.broadcast(localSamples)

    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    rdd
      .map { case (v, gs) =>
        val serializer = SparkEnv.get.serializer.newInstance()
        val zeroValue = serializer.deserialize[U](ByteBuffer.wrap(zeroArray))

        (v, gs.zipWithIndex.foldLeft(zeroValue) { case (acc, (g, i)) =>
          seqOp(acc, v, localSamplesBc.value(i), g)
        })
      }
  }

  def foldBySample(zeroValue: T)(combOp: (T, T) => T): RDD[(Int, T)] = {

    val localSamplesBc = sparkContext.broadcast(localSamples)
    val localtct = tct

    val serializer = SparkEnv.get.serializer.newInstance()
    val zeroBuffer = serializer.serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    rdd
      .mapPartitions { (it: Iterator[(Variant, S)]) =>
        val serializer = SparkEnv.get.serializer.newInstance()
        def copyZeroValue() = serializer.deserialize[T](ByteBuffer.wrap(zeroArray))(localtct)
        val arrayZeroValue = Array.fill[T](localSamplesBc.value.size)(copyZeroValue())
        localSamplesBc.value.iterator
          .zip(it.foldLeft(arrayZeroValue) { case (acc, (v, gs)) =>
            for ((g, i) <- gs.zipWithIndex)
              acc(i) = combOp(acc(i), g)
            acc
          }.iterator)
      }.foldByKey(zeroValue)(combOp)
  }

  def foldByVariant(zeroValue: T)(combOp: (T, T) => T): RDD[(Variant, T)] = {
    rdd
      .mapValues(_.foldLeft(zeroValue)((acc, g) => combOp(acc, g)))
  }

  def fullOuterJoin(other:RDD[(Variant,S)]): RDD[(Variant,(Option[S],Option[S]))] = {
    rdd.fullOuterJoin(other)
  }

  def leftOuterJoin(other:RDD[(Variant,S)]): RDD[(Variant, (S,Option[S]))] = {
    rdd.leftOuterJoin(other)
  }

  def rightOuterJoin(other:RDD[(Variant,S)]): RDD[(Variant, (Option[S],S))] = {
    rdd.rightOuterJoin(other)
  }

  def innerJoin(other:RDD[(Variant,S)]): RDD[(Variant, (S,S))] = {
    rdd.join(other)
  }
}
