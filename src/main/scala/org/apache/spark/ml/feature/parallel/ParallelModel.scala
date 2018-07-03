/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.{Estimator, Model, Transformer}

/**
  * ParallelModel
  *
  * Parallel [[Model]] in that transformations are executed in parallel.
  * Generally created by calling fit() on a [[ParallelEstimator]].
  * This is based on the [[Model]] class.
  *
  * @tparam M child model type
  *
  * @author belbis
  * @since 1.0.0
  */
trait ParallelModel[M <: Model[M]] extends ParallelTransformer {

  /**
    * The parent estimators that produced this model.
    *
    * @note For ensembles' component Models, this value can be null.
    *
    */
  @transient var parents: Map[String, Estimator[M]] = _

  /**
    * setParents
    *
    * setter for parents
    *
    * @param value
    * @return
    */
  def setParents(value: Map[String, Estimator[M]]): this.type = {
    parents = value
    this
  }

  /** Indicates whether this [[Model]] has corresponding parents. */
  def hasParents: Boolean = parents != null && parents.nonEmpty

  override def setOutputCols(value: Array[String]): this.type = {
    val err = "outputCols should be set in the Estimator."
    throw new RuntimeException(err)
  }

  override def createStage(inputColumn: String): Transformer = {
    val err = "ParallelModel objects cannot create stages. Use the corresponding estimator."
    throw new RuntimeException(err)
  }

}
