/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.param.{BooleanParam, Params}
import org.apache.spark.ml.util.Identifiable

/**
  * ParallelOneHotEncoderParams
  *
  * [[org.apache.spark.ml.param.Params]] to use for [[ParallelOneHotEncoder]].
  *
  * @author belbis
  * @since 1.0.0
  */
trait ParallelOneHotEncoderParams extends Params {

  /**
    * Whether to drop the last category in the encoded vector (default: true)
    */
  final val dropLast: BooleanParam = new BooleanParam(this, "dropLast", "whether to drop the " +
    "last category")

  setDefault(dropLast -> true)

  def getDropLast: Boolean = $(dropLast)

}

/**
  * ParallelOneHotEncoder
  *
  * [[org.apache.spark.ml.Model]] output from [[ParallelOneHotEncoder]]. Used for term frequency counting of multiple
  * columns.
  *
  * @param uid
  * @author belbis
  * @since 1.0.0
  */
class ParallelOneHotEncoder(override val uid: String) extends ParallelTransformer
  with ParallelOneHotEncoderParams {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelOneHotEncoder"))

  /**
    * createStage
    *
    * Create new instance of [[OneHotEncoder]].
    *
    * @param inputColumn
    * @return
    */
  override def createStage(inputColumn: String): OneHotEncoder = {
    new OneHotEncoder()
      .setInputCol(inputColumn)
      .setOutputCol($(outputCols)($(inputCols).indexOf(inputColumn)))
      .setDropLast($(dropLast))
  }

}
