/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * ParallelTransformer
  *
  * [[Transformer]] that operates on multiple input columns.
  *
  * @author belbis
  * @since 1.0.0
  */
trait ParallelTransformer extends Transformer with ParallelPipelineStage[Transformer] {

  /**
    * transform
    *
    * Apply each stage in parallel
    *
    * @param dataset
    * @return
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val ds = if ($(intermediateCache)) dataset.persist else dataset
    val out = $(inputCols).par.foldLeft(ds.toDF) {
      case (foldDF, inputCol) => $(stages)(inputCol).transform(foldDF)
    }
    if ($(intermediateCache)) ds.unpersist
    out
  }

  override def transformSchema(schema: StructType): StructType = transformSchemaUsingStages(schema)

}
