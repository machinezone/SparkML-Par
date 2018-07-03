/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamValidators, Params}
import org.apache.spark.ml.util.Identifiable

/**
  * ParallelHashingTFParams
  *
  * [[org.apache.spark.ml.param.Params]] to use for [[ParallelHashingTF]].
  *
  * @author belbis
  * @since 1.0.0
  */
trait ParallelHashingTFParams extends Params {

  /**
    * Number of features. Should be greater than 0.
    * (default = 2^18^)
    */
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))

  /**
    * Binary toggle to control term frequency counts.
    * If true, all non-zero counts are set to 1.  This is useful for discrete probabilistic
    * models that model binary events rather than integer counts.
    * (default = false)
    *
    */
  val binary = new BooleanParam(this, "binary", "If true, all non zero counts are set to 1. " +
    "This is useful for discrete probabilistic models that model binary events rather " +
    "than integer counts")

  setDefault(numFeatures -> (1 << 18), binary -> false)

  def getNumFeatures: Int = $(numFeatures)

  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  def getBinary: Boolean = $(binary)

  def setBinary(value: Boolean): this.type = set(binary, value)

}

/**
  * ParallelHashingTF
  *
  * [[org.apache.spark.ml.Model]] output from [[ParallelHashingTF]]. Used for term frequency counting of multiple
  * columns.
  *
  * @param uid
  * @author belbis
  * @since 1.0.0
  */
class ParallelHashingTF(override val uid: String) extends ParallelTransformer
  with ParallelHashingTFParams {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelHashingTF"))

  /**
    * createStage
    *
    * Create new instance of [[HashingTF]] to be used on the input column.
    *
    * @param inputColumn
    * @return
    */
  override def createStage(inputColumn: String): HashingTF = {
    new HashingTF()
      .setInputCol(inputColumn)
      .setOutputCol($(outputCols)($(inputCols).indexOf(inputColumn)))
      .setBinary($(binary))
      .setNumFeatures($(numFeatures))
  }

}
