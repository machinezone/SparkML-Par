/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

/**
  * ParallelTokenizerTest
  *
  * Unit tests for [[ParallelTokenizer]].
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelTokenizerTest extends TestBase {

  "#transform()" should "transform a dataframe" in {
    val schema = StructType(Seq(
      StructField("_1", DataTypes.IntegerType),
      StructField("_2", DataTypes.StringType),
      StructField("_3", DataTypes.StringType),
      StructField("_4", DataTypes.StringType)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(1, "all you need all love you", "whether 'tis nobler in the mind", "mortal coil"),
      Row(2, "need,you, is,one,love", "that is the question",
        "california,love", "to be or not to be")
    )), schema)

    val tokenizer = new ParallelTokenizer()
      .setInputCols(Array("_2", "_3", "_4"))
      .setOutputCols(Array("_2_tok", "_3_tok", "_4_tok"))
    val out = tokenizer.transform(df).select("_2_tok", "_3_tok", "_4_tok").collect


    out(0)(0) should equal(Array("all", "you", "need", "all", "love", "you"))
    out(0)(1) should equal(Array("whether", "'tis", "nobler", "in", "the", "mind"))
    out(0)(2) should equal(Array("mortal", "coil"))

    out(1)(0) should equal(Array("need,you,", "is,one,love"))
    out(1)(1) should equal(Array("that", "is", "the", "question"))
    out(1)(2) should equal(Array("california,love"))
  }

  "#transformSchema" should "add columns" in {
    val testSchema = StructType(Seq(
      StructField("_1", DataTypes.StringType),
      StructField("_2", DataTypes.StringType)
    ))

    val prt = new ParallelTokenizer()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_tok", "_2_tok"))
    val outSchema = prt.transformSchema(testSchema)

    val firstField = outSchema(2)
    firstField.dataType should equal(ArrayType(DataTypes.StringType, containsNull = true))
    firstField.name should equal("_1_tok")

    val secondField = outSchema(3)
    secondField.dataType should equal(ArrayType(DataTypes.StringType, containsNull = true))
    secondField.name should equal("_2_tok")
  }

}
