/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.util.Identifiable

/**
  * ParallelTokenizer
  *
  * Parallel version of simple [[Tokenizer]]. Works on multiple input columns.
  *
  * @param uid
  * @author belbis
  * @since 1.0.0
  */
class ParallelTokenizer(override val uid: String) extends ParallelTransformer {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelTokenizer"))

  /**
    * createStage
    *
    * Create new instance of [[Tokenizer]].
    *
    * @param inputColumn
    * @return
    */
  override def createStage(inputColumn: String): Tokenizer = {
    new Tokenizer()
      .setInputCol(inputColumn)
      .setOutputCol($(outputCols)($(inputCols).indexOf(inputColumn)))
  }

}
