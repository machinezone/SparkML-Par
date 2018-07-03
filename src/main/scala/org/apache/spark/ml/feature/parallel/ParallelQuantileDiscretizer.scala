/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer, QuantileDiscretizerBase}
import org.apache.spark.ml.util.Identifiable

/**
  * ParallelQuantileDiscretizer
  *
  * Parallel implementation of [[QuantileDiscretizer]].
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelQuantileDiscretizer(override val uid: String)
  extends ParallelEstimator[ParallelBucketizer, Bucketizer]
    with QuantileDiscretizerBase {

  def setRelativeError(value: Double): this.type = set(relativeError, value)

  def setNumBuckets(value: Int): this.type = set(numBuckets, value)

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelQuantileDiscretizer"))

  override def createStage(inputColumn: String): QuantileDiscretizer = {
    new QuantileDiscretizer()
      .setNumBuckets($(numBuckets))
      .setRelativeError($(relativeError))
      .setInputCol(inputColumn)
      .setOutputCol($(outputCols)($(inputCols).indexOf(inputColumn)))
  }

  override def createModel(models: Map[String, Bucketizer]): ParallelBucketizer = {
    new ParallelBucketizer()
      .setInputCols($(inputCols))
      .setStages(models)
  }

}

