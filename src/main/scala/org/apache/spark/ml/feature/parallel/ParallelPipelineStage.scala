/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.parallel.HasInputOutputCols
import org.apache.spark.ml.param.{BooleanParam, Param, ParamMap, Params}
import org.apache.spark.sql.types.StructType

/**
  * ParallelPipelineStage
  *
  * To be used with  [[PipelineStage]]s that operate on multiple input columns.
  *
  * @tparam T
  * @author belbis
  * @since 1.0.0
  */
trait ParallelPipelineStage[T <: PipelineStage] extends Params with HasInputOutputCols {

  val stages = new Param[Map[String, T]](this, "stages", "Map of stages to input columns")
  val intermediateCache = new BooleanParam(this, "intermediateCache", "Cache input dataset" +
    "to improve parallel pipeline stage performance.")

  def getStages: Map[String, T] = $(stages)

  setDefault(stages, Map.empty[String, T])

  def setIntermediateCache(value: Boolean): this.type = set(intermediateCache, value)

  def getIntermediateCache: Boolean = $(intermediateCache)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  setDefault(intermediateCache, false)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  /**
    * createStage
    *
    * Given an input column, generate a [[PipelineStage]] to be used.
    *
    * @param inputColumn
    * @return
    */
  def createStage(inputColumn: String): T

  /**
    * transformSchemaUsingStages
    *
    * transform the input schema using the stored [[PipelineStage]] objects.
    *
    * NOTE: this does not transform the schema in parallel, it is only a helper
    * method for the [[ParallelPipelineStage]] to transform the schema
    *
    * @param schema
    * @return
    */
  def transformSchemaUsingStages(schema: StructType): StructType = {
    populateStages()
    $(inputCols).foldLeft(schema) {
      case (sch, input) => $(stages)(input).transformSchema(sch)
    }
  }

  /**
    * populateStages
    *
    * Ensure that a [[PipelineStage]] exists for each of the input columns either by using the
    * provided [[PipelineStage]] or creating a new one with [[Param]]s defined in the implementing class.
    */
  def populateStages(): Unit = {
    val populatedStages = $(inputCols).map { col =>
      val stage = $(stages).getOrElse(col, createStage(col))
      col -> stage
    }.toMap
    setStages(populatedStages)
  }

  /**
    * setStages
    *
    * setter for [[PipelineStage]]s.
    *
    * @param value
    * @return
    */
  def setStages(value: Map[String, T]): this.type = set(stages, value)

  /**
    * copy
    *
    * Use defaultCopy by default.
    *
    * @param extra
    * @return
    */
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

}
