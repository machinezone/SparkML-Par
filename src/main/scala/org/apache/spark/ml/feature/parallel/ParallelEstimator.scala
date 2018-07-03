/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

/**
  * ParallelEstimator
  *
  * [[Estimator]] that operates in parallel on multiple input columns.
  *
  * @tparam M
  * @author belbis
  * @since 1.0.0
  */
trait ParallelEstimator[M <: Model[M], N <: Model[N]] extends Estimator[M]
  with ParallelPipelineStage[Estimator[N]] {

  /**
    * createModel
    *
    * Create an instance of [[Model]] using the child models.
    *
    * @param models
    * @return
    */
  def createModel(models: Map[String, N]): M

  /**
    * fit
    *
    * Generate a Model for each of the input columns specified.
    *
    * @param dataset
    * @return
    */
  override def fit(dataset: Dataset[_]): M = {
    transformSchema(dataset.schema, logging = true)
    val ds = if ($(intermediateCache)) dataset.persist else dataset
    val models = $(inputCols).par.map { columnName =>
      val estimator = $(stages)(columnName)
      columnName -> estimator.fit(ds)
    }.seq.toMap

    if ($(intermediateCache)) ds.unpersist

    createModel(models)
  }

  override def transformSchema(schema: StructType): StructType = transformSchemaUsingStages(schema)

}
