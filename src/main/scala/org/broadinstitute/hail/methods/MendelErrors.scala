package org.broadinstitute.hail.methods

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.utils.MultiArray2
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.variant.GenotypeType._

case class MendelError(variant: Variant, trio: CompleteTrio, code: Int,
                       gtKid: GenotypeType, gtDad: GenotypeType, gtMom: GenotypeType) {

  def gtString(v: Variant, gt: GenotypeType): String =
    if (gt == HomRef)
      v.ref + "/" + v.ref
    else if (gt == Het)
      v.ref + "/" + v.alt
    else if (gt == HomVar)
      v.alt + "/" + v.alt
    else
      "./."

  def implicatedSamples: Iterator[Int] =
    if      (code == 2 || code == 1)                             Iterator(trio.kid, trio.dad, trio.mom)
    else if (code == 6 || code == 3)                             Iterator(trio.kid, trio.dad)
    else if (code == 4 || code == 7 || code == 9 || code == 10)  Iterator(trio.kid, trio.mom)
    else                                                         Iterator(trio.kid)

  def toLineMendel(sampleIds: IndexedSeq[String]): String = {
    val v = variant
    val t = trio
    val errorString = gtString(v, gtDad) + " x " + gtString(v, gtMom) + " -> " + gtString(v, gtKid)
    t.fam.getOrElse("0") + "\t" + sampleIds(t.kid) + "\t" + v.contig + "\t" +
      v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt + "\t" + code + "\t" + errorString
  }
}

object MendelErrors {

  def getCode(gts: IndexedSeq[GenotypeType], isHemizygous: Boolean): Int = {
    (gts(1), gts(2), gts(0), isHemizygous) match { // gtDad, gtMom, gtKid, isHemizygous
      case (HomRef, HomRef,    Het, false) => 2    // Kid is het and not hemizygous
      case (HomVar, HomVar,    Het, false) => 1
      case (HomRef, HomRef, HomVar, false) => 5    // Kid is homvar and not hemizygous
      case (HomRef,      _, HomVar, false) => 3
      case (     _, HomRef, HomVar, false) => 4
      case (HomVar, HomVar, HomRef, false) => 8    // Kid is homref and not hemizygous
      case (HomVar,      _, HomRef, false) => 6
      case (     _, HomVar, HomRef, false) => 7
      case (     _, HomVar, HomRef,  true) => 9    // Kid is homref and hemizygous
      case (     _, HomRef, HomVar,  true) => 10   // Kid is homvar and hemizygous
      case _                               => 0    // No error
    }
  }

  def apply(vds: VariantDataset, trios: Array[CompleteTrio]): MendelErrors = {
    require(trios.forall(_.sex.isDefined))

    val sampleTrioRoles: Array[List[(Int, Int)]] = Array.fill[List[(Int, Int)]](vds.nSamples)(List())
    trios.zipWithIndex.foreach { case (t, ti) =>
      sampleTrioRoles(t.kid) ::= (ti, 0)
      sampleTrioRoles(t.dad) ::= (ti, 1)
      sampleTrioRoles(t.mom) ::= (ti, 2)
    }

    val sc = vds.sparkContext
    val sampleTrioRolesBc = sc.broadcast(sampleTrioRoles)
    val triosBc = sc.broadcast(trios)
    // all trios have defined sex, see require above
    val trioSexBc = sc.broadcast(trios.map(_.sex.get))

    val zeroVal: MultiArray2[GenotypeType] = MultiArray2.fill(trios.length,3)(NoCall)

    def seqOp(a: MultiArray2[GenotypeType], s: Int, g: Genotype): MultiArray2[GenotypeType] = {
      sampleTrioRolesBc.value(s).foreach{ case (ti, ri) => a.update(ti,ri,g.gtType) }
      a
    }

    def mergeOp(a: MultiArray2[GenotypeType], b: MultiArray2[GenotypeType]): MultiArray2[GenotypeType] = {
      for ((i,j) <- a.indices)
        if (b(i,j) != NoCall)
          a(i,j) = b(i,j)
      a
    }

    new MendelErrors(trios, vds.sampleIds,
      vds
        .aggregateByVariantWithKeys(zeroVal)(
          (a, v, s, g) => seqOp(a, s, g),
          mergeOp)
        .flatMap { case (v, a) =>
          a.rows.flatMap { case (row) => val code = getCode(row, v.isHemizygous(trioSexBc.value(row.i)))
            if (code != 0)
              Some(new MendelError(v, triosBc.value(row.i), code, row(0), row(1), row(2)))
            else
              None
          }
        }
        .cache()
    )
  }
}

case class MendelErrors(trios:        Array[CompleteTrio],
                        sampleIds:    IndexedSeq[String],
                        mendelErrors: RDD[MendelError]) {

  val sc = mendelErrors.sparkContext
  val trioFam = trios.iterator.flatMap(t => t.fam.map(f => (t.kid, f))).toMap
  val nuclearFams = Pedigree.nuclearFams(trios)

  def nErrorPerVariant: RDD[(Variant, Int)] = {
    mendelErrors
      .map(_.variant)
      .countByValueRDD()
  }

  def nErrorPerNuclearFamily: RDD[((Int, Int), Int)] = {
    val parentsRDD = sc.parallelize(nuclearFams.keys.toSeq)
    mendelErrors
      .map(me => ((me.trio.dad, me.trio.mom), 1))
      .union(parentsRDD.map((_, 0)))
      .reduceByKey(_ + _)
  }

  def nErrorPerIndiv: RDD[(Int, Int)] = {
    val indivRDD = sc.parallelize(trios.flatMap(t => Iterator(t.kid, t.dad, t.mom)).distinct)
    mendelErrors
      .flatMap(_.implicatedSamples)
      .map((_, 1))
      .union(indivRDD.map((_, 0)))
      .reduceByKey(_ + _)
  }

  def writeMendel(filename: String) {
    val sampleIdsBc = sc.broadcast(sampleIds)
    mendelErrors.map(_.toLineMendel(sampleIdsBc.value))
      .writeTable(filename, Some("FID\tKID\tCHR\tSNP\tCODE\tERROR"))
  }

  def writeMendelL(filename: String) {
    nErrorPerVariant.map{ case (v, n) =>
      v.contig + "\t" + v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt + "\t" + n
    }.writeTable(filename, Some("CHR\tSNP\tN"))
  }

  def writeMendelF(filename: String) {
    val trioFamBc = sc.broadcast(trioFam)
    val nuclearFamsBc = sc.broadcast(nuclearFams)
    val sampleIdsBc = sc.broadcast(sampleIds)
    val lines = nErrorPerNuclearFamily.map{ case ((dad, mom), n) =>
      trioFamBc.value.getOrElse(dad, "0") + "\t" + sampleIdsBc.value(dad) + "\t" + sampleIdsBc.value(mom) + "\t" +
        nuclearFamsBc.value((dad, mom)).size + "\t" + n
    }.collect()
    writeTable(filename, sc.hadoopConfiguration, lines, "FID\tPAT\tMAT\tCHLD\tN")
  }

  def writeMendelI(filename: String) {
    val trioFamBc = sc.broadcast(trioFam)
    val sampleIdsBc = sc.broadcast(sampleIds)
    val lines = nErrorPerIndiv.map { case (s, n) =>
      trioFamBc.value.getOrElse(s, "0") + "\t" + sampleIdsBc.value(s) + "\t" + n
    }.collect()
    writeTable(filename, sc.hadoopConfiguration, lines, "FID\tIID\tN")
  }
}
