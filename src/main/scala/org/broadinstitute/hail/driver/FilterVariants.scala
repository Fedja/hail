package org.broadinstitute.hail.driver

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.methods._
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.annotations._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable.ArrayBuffer

object FilterVariants extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "--all", usage = "Filter all variants")
    var all: Boolean = false

    @Args4jOption(required = false, name = "-c", aliases = Array("--condition"),
      usage = "Filter condition: expression, .interval_list or .variant_list file")
    var condition: String = _

    @Args4jOption(required = false, name = "--keep", usage = "Keep variants matching condition")
    var keep: Boolean = false

    @Args4jOption(required = false, name = "--remove", usage = "Remove variants matching condition")
    var remove: Boolean = false

  }

  def newOptions = new Options

  def name = "filtervariants"

  def description = "Filter variants in current dataset"

  override def supportsMultiallelic = true

  def run(state: State, options: Options): State = {
    val vds = state.vds

    if ((options.keep && options.remove)
      || (!options.keep && !options.remove))
      fatal("one `--keep' or `--remove' required, but not both")

    if ((options.all && options.condition != null)
      || (!options.all && options.condition == null))
      fatal("one `--all' or `-c' required, but not both")

    if (options.all) {
      if (options.keep)
        return state
      else
        return state.copy(
          vds = state.vds.copy(rdd = state.sc.emptyRDD))
    }

    val vas = vds.vaSignature
    val cond = options.condition
    val keep = options.keep
    val p: (Variant, Annotation) => Boolean = cond match {
      case f if f.endsWith(".interval_list") =>
        val ilist = IntervalList.read(options.condition, state.hadoopConf)
        val ilistBc = state.sc.broadcast(ilist)
        (v: Variant, va: Annotation) => Filter.keepThis(ilistBc.value.contains(v.contig, v.start), keep)

      case f if f.endsWith(".variant_list") =>
        val variants: RDD[(Variant, Unit)] =
          vds.sparkContext.textFile(f)
            .map { line =>
              val fields = line.split(":")
              if (fields.length != 4)
                fatal("invalid variant")
              val ref = fields(2)
              (Variant(fields(0),
                fields(1).toInt,
                ref,
                fields(3).split(",").map(alt => AltAllele(ref, alt))), ())
            }

        return state.copy(
          vds = vds.copy(
            rdd =
              if (options.keep)
                vds.rdd
                  .map { case (v, va, gs) => (v, (va, gs)) }
                  .join(variants)
                  .map { case (v, ((va, gs), _)) => (v, va, gs) }
              else
                vds.rdd
                  .map { case (v, va, gs) => (v, (va, gs)) }
                  .leftOuterJoin(variants)
                  .flatMap {
                    case (v, ((va, gs), Some(_))) =>
                      None
                    case (v, ((va, gs), None)) =>
                      Some((v, va, gs))
                  }
          ))

      case c: String =>
        val symTab = Map(
          "v" ->(0, TVariant),
          "va" ->(1, vas))
        val a = new ArrayBuffer[Any]()
        for (_ <- symTab)
          a += null
        val f: () => Option[Boolean] = Parser.parse[Boolean](cond, symTab, a, TBoolean)
        (v: Variant, va: Annotation) => {
          a(0) = v
          a(1) = va
          Filter.keepThis(f(), keep)
        }
    }

    state.copy(vds = vds.filterVariants(p))
  }
}
