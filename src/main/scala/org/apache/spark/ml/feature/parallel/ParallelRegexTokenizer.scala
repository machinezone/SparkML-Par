/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable

/**
  * ParallelRegexTokenizerParams
  *
  * [[org.apache.spark.ml.param.Params]] to use for [[ParallelRegexTokenizer]].
  *
  * @author belbis
  * @since 1.0.0
  */
trait ParallelRegexTokenizerParams extends Params {

  /**
    * Indicates whether to convert all characters to lowercase before tokenizing.
    * Default: true
    *
    */
  final val toLowercase: BooleanParam = new BooleanParam(this, "toLowercase",
    "whether to convert all characters to lowercase before tokenizing.")
  /**
    * Minimum token length, greater than or equal to 0.
    * Default: 1, to avoid returning empty strings
    *
    */
  val minTokenLength: IntParam = new IntParam(this, "minTokenLength", "minimum token length (>= 0)",
    ParamValidators.gtEq(0))
  /**
    * Indicates whether regex splits on gaps (true) or matches tokens (false).
    * Default: true
    *
    */
  val gaps: BooleanParam = new BooleanParam(this, "gaps", "Set regex to match gaps or tokens")
  /**
    * Regex pattern used to match delimiters if [[gaps]] is true or tokens if [[gaps]] is false.
    * Default: `"\\s+"`
    *
    */
  val pattern: Param[String] = new Param(this, "pattern", "regex pattern used for tokenizing")

  def setMinTokenLength(value: Int): this.type = set(minTokenLength, value)

  def getMinTokenLength: Int = $(minTokenLength)

  def setGaps(value: Boolean): this.type = set(gaps, value)

  def getGaps: Boolean = $(gaps)

  def setPattern(value: String): this.type = set(pattern, value)

  def getPattern: String = $(pattern)

  def setToLowercase(value: Boolean): this.type = set(toLowercase, value)

  def getToLowercase: Boolean = $(toLowercase)

  setDefault(minTokenLength -> 1, gaps -> true, pattern -> "\\s+", toLowercase -> true)

}

/**
  * ParallelRegexTokenizer
  *
  * [[ParallelTransformer]] that performs regex tokenization.
  *
  * @param uid
  * @author belbis
  * @since 1.0.0
  */
class ParallelRegexTokenizer(override val uid: String) extends ParallelTransformer
  with ParallelRegexTokenizerParams {

  // NOTE: uid must be defined in the constructor or you will see errors a la
  // https://issues.apache.org/jira/browse/SPARK-12606
  def this() = this(Identifiable.randomUID("ParallelRegexTokenizer"))

  /**
    * createStage
    *
    * Create new instance of [[RegexTokenizer]].
    *
    * @param inputColumn
    * @return
    */
  override def createStage(inputColumn: String): RegexTokenizer = {
    new RegexTokenizer()
      .setInputCol(inputColumn)
      .setOutputCol($(outputCols)($(inputCols).indexOf(inputColumn)))
      .setGaps($(gaps))
      .setMinTokenLength($(minTokenLength))
      .setPattern($(pattern))
      .setToLowercase($(toLowercase))
  }

}
