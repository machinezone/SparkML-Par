/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.Model
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, CountVectorizerParams}
import org.apache.spark.ml.util.Identifiable

/**
  * ParallelCountVectorizer
  *
  * [[CountVectorizer]] that handles multiple input columns in parallel.
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelCountVectorizer(override val uid: String)
  extends ParallelEstimator[ParallelCountVectorizerModel, CountVectorizerModel]
    with CountVectorizerParams {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelCountVectorizer"))

  def setVocabSize(value: Int): this.type = set(vocabSize, value)

  def setMinDF(value: Double): this.type = set(minDF, value)

  def setMinTF(value: Double): this.type = set(minTF, value)

  def setBinary(value: Boolean): this.type = set(binary, value)

  /**
    * createStage
    *
    * Create an instance of [[CountVectorizer]].
    *
    * @param inputColumn
    * @return
    */
  override def createStage(inputColumn: String): CountVectorizer = {
    new CountVectorizer()
      .setInputCol(inputColumn)
      .setOutputCol($(outputCols)($(inputCols).indexOf(inputColumn)))
      .setMinDF($(minDF))
      .setMinTF($(minTF))
      .setVocabSize($(vocabSize))
  }

  /**
    * createModel
    *
    * create instance of [[ParallelCountVectorizerModel]].
    *
    * @param models
    * @return
    */
  override def createModel(models: Map[String, CountVectorizerModel]):
  ParallelCountVectorizerModel = {
    new ParallelCountVectorizerModel(uid)
      .setInputCols($(inputCols))
      .setStages(models)
  }

}

/**
  * ParallelCountVectorizerModel
  *
  * [[Model]] output from [[ParallelCountVectorizer]]. Used for term frequency counting
  * of multiple columns.
  *
  * @param uid
  */
class ParallelCountVectorizerModel(override val uid: String)
  extends Model[ParallelCountVectorizerModel] with ParallelModel[CountVectorizerModel]
    with CountVectorizerParams {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelCountVectorizerModel"))

}
