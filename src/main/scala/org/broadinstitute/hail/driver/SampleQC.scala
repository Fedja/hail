package org.broadinstitute.hail.driver

import org.apache.commons.math3.distribution.BinomialDistribution
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter
import org.broadinstitute.hail.methods._
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.Utils._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable

object SampleQCCombiner {
  val header = "nCalled\t" +
    "nNotCalled\t" +
    "nHomRef\t" +
    "nHet\t" +
    "nHomVar\t" +
    "nSNP\t" +
    "nInsertion\t" +
    "nDeletion\t" +
    "nSingleton\t" +
    "nTransition\t" +
    "nTransversion\t" +
    "dpMean\tdpStDev\t" +
    "dpMeanHomRef\tdpStDevHomRef\t" +
    "dpMeanHet\tdpStDevHet\t" +
    "dpMeanHomVar\tdpStDevHomVar\t" +
    "gqMean\tgqStDev\t" +
    "gqMeanHomRef\tgqStDevHomRef\t" +
    "gqMeanHet\tgqStDevHet\t" +
    "gqMeanHomVar\tgqStDevHomVar\t" +
    "nNonRef\t" +
    "rTiTv\t" +
    "rHetHomVar\t" +
    "rDeletionInsertion"
}

class SampleQCCombiner extends Serializable {
  var nNotCalled: Int = 0
  var nHomRef: Int = 0
  var nHet: Int = 0
  var nHomVar: Int = 0
  var refDepth: Int = 0
  var altDepth: Int = 0

  val dpSC = new StatCounter()
  val dpHomRefSC = new StatCounter()
  val dpHetSC = new StatCounter()
  val dpHomVarSC = new StatCounter()

  var nSNP: Int = 0
  var nIns: Int = 0
  var nDel: Int = 0
  var nSingleton: Int = 0
  var nTi: Int = 0
  var nTv: Int = 0

  val gqSC: StatCounter = new StatCounter()
  val gqHomRefSC: StatCounter = new StatCounter()
  val gqHetSC: StatCounter = new StatCounter()
  val gqHomVarSC: StatCounter = new StatCounter()

  // FIXME per-genotype

  def merge(v: Variant, vIsSingleton: Boolean, g: Genotype): SampleQCCombiner = {
    g.call.map(_.gt) match {
      case Some(0) =>
        nHomRef += 1
        dpSC.merge(g.dp)
        dpHomRefSC.merge(g.dp)
        gqSC.merge(g.gq)
        gqHomRefSC.merge(g.gq)
      case Some(1) =>
        nHet += 1
        refDepth += g.ad._1
        altDepth += g.ad._2
        if (v.isSNP) {
          nSNP += 1
          if (v.isTransition)
            nTi += 1
          else {
            assert(v.isTransversion)
            nTv += 1
          }
        } else if (v.isInsertion)
          nIns += 1
        else if (v.isDeletion)
          nDel += 1
        if (vIsSingleton)
          nSingleton += 1
        dpSC.merge(g.dp)
        dpHetSC.merge(g.dp)
        gqSC.merge(g.gq)
        gqHetSC.merge(g.gq)
      case Some(2) =>
        nHomVar += 1
        if (v.isSNP) {
          nSNP += 1
          if (v.isTransition)
            nTi += 1
          else {
            assert(v.isTransversion)
            nTv += 1
          }
        } else if (v.isInsertion)
          nIns += 1
        else if (v.isDeletion)
          nDel += 1
        if (vIsSingleton)
          nSingleton += 1
        dpSC.merge(g.dp)
        dpHomVarSC.merge(g.dp)
        gqSC.merge(g.gq)
        gqHomVarSC.merge(g.gq)
      case None =>
        nNotCalled += 1
      case _ =>
        throw new IllegalArgumentException("Genotype value " + g.call.map(_.gt).get + " must be 0, 1, or 2.")
    }

    this
  }

  def merge(that: SampleQCCombiner): SampleQCCombiner = {
    nNotCalled += that.nNotCalled
    nHomRef += that.nHomRef
    nHet += that.nHet
    nHomVar += that.nHomVar
    refDepth += that.refDepth
    altDepth += that.altDepth

    nSNP += that.nSNP
    nIns += that.nIns
    nDel += that.nDel
    nSingleton += that.nSingleton
    nTi += that.nTi
    nTv += that.nTv

    dpSC.merge(that.dpSC)
    dpHomRefSC.merge(that.dpHomRefSC)
    dpHetSC.merge(that.dpHetSC)
    dpHomVarSC.merge(that.dpHomVarSC)

    gqSC.merge(that.gqSC)
    gqHomRefSC.merge(that.gqHomRefSC)
    gqHetSC.merge(that.gqHetSC)
    gqHomVarSC.merge(that.gqHomVarSC)

    this
  }

  def emitSC(sb: mutable.StringBuilder, sc: StatCounter) {
    sb.tsvAppend(someIf(sc.count > 0, sc.mean))
    sb += '\t'
    sb.tsvAppend(someIf(sc.count > 0, sc.stdev))
  }

  def emit(sb: mutable.StringBuilder) {
    val nCalled = nHomRef + nHet + nHomVar

    sb.append(nCalled)
    sb += '\t'
    sb.append(nNotCalled)
    sb += '\t'
    sb.append(nHomRef)
    sb += '\t'
    sb.append(nHet)
    sb += '\t'
    sb.append(nHomVar)
    sb += '\t'

    sb.append(nSNP)
    sb += '\t'
    sb.append(nIns)
    sb += '\t'
    sb.append(nDel)
    sb += '\t'

    sb.append(nSingleton)
    sb += '\t'

    sb.append(nTi)
    sb += '\t'
    sb.append(nTv)
    sb += '\t'


    emitSC(sb, dpSC)
    sb += '\t'
    emitSC(sb, dpHomRefSC)
    sb += '\t'
    emitSC(sb, dpHetSC)
    sb += '\t'
    emitSC(sb, dpHomVarSC)
    sb += '\t'

    emitSC(sb, gqSC)
    sb += '\t'
    emitSC(sb, gqHomRefSC)
    sb += '\t'
    emitSC(sb, gqHetSC)
    sb += '\t'
    emitSC(sb, gqHomVarSC)
    sb += '\t'

    // nNonRef
    sb.append(nHet + nHomVar)
    sb += '\t'

    // nTiTvf
    sb.tsvAppend(divOption(nTi, nTv))
    sb += '\t'

    // rHetHomVar
    sb.tsvAppend(divOption(nHet, nHomVar))
    sb += '\t'

    // rDeletionInsertion
    sb.tsvAppend(divOption(nDel, nIns))
  }
}

object SampleQC extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output file")
    var output: String = _
  }

  def newOptions = new Options

  def name = "sampleqc"

  def description = "Compute per-sample QC metrics"

  def results(vds: VariantDataset): RDD[(Int, SampleQCCombiner)] = {

    /*
    val singletons = sSingletonVariants(vds)
    val singletonsBc = vds.sparkContext.broadcast(singletons)
    vds
      .aggregateBySampleWithKeys(new SampleQCCombiner)(
        (comb, v, s, g) => comb.merge(v, singletonsBc.value(v), g),
        (comb1, comb2) => comb1.merge(comb2))
        */

    val localSamplesBc = vds.sparkContext.broadcast(vds.localSamples)
    vds
      .rdd
      .mapPartitions[(Int, SampleQCCombiner)] { (it: Iterator[(Variant, Iterable[Genotype])]) =>
        val zeroValue = Array.fill[SampleQCCombiner](localSamplesBc.value.length)(new SampleQCCombiner)
        localSamplesBc.value.iterator
          .zip(it.foldLeft(zeroValue) { case (acc, (v, gs)) =>
            val vIsSingleton = gs.iterator.existsExactly1(_.isCalledNonRef)
            for ((g, i) <- gs.zipWithIndex)
              acc(i) = acc(i).merge(v, vIsSingleton, g)
            acc
          }.iterator)
      }.foldByKey(new SampleQCCombiner)((comb1, comb2) => comb1.merge(comb2))
  }

  def run(state: State, options: Options): State = {
    val vds = state.vds

    val output = options.output

    writeTextFile(output + ".header", state.hadoopConf) { s =>
      s.write("sampleID\t")
      s.write(SampleQCCombiner.header)
      s.write("\n")
    }

    val sampleIdsBc = state.sc.broadcast(vds.sampleIds)

    hadoopDelete(output, state.hadoopConf, true)
    val r = results(vds)
      .map { case (s, comb) =>
        val sb = new StringBuilder()
        sb.append(sampleIdsBc.value(s))
        sb += '\t'
        comb.emit(sb)
        sb.result()
      }.saveAsTextFile(output)

    state
  }
}
