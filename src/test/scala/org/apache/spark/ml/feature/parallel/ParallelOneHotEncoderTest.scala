/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * ParallelOneHotEncoderTest
  *
  * Unit tests for [[ParallelOneHotEncoder]]
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelOneHotEncoderTest extends TestBase {


  "#transform()" should "transform a dataframe" in {
    val df = spark.createDataFrame(Seq(
      (0, "a", "b", 0.0, 1.0),
      (1, "b", "c", 2.0, 0.0),
      (2, "c", "b", 1.0, 1.0),
      (3, "a", "c", 0.0, 0.0),
      (4, "a", "a", 0.0, 2.0),
      (5, "c", "c", 1.0, 0.0)
    )).toDF("_1", "_2", "_3", "_4", "_5")

    val pohe = new ParallelOneHotEncoder()
      .setInputCols(Array("_4", "_5"))
      .setOutputCols(Array("_4_ohe", "_5_ohe"))

    val out = pohe.transform(df).select("_4_ohe", "_5_ohe").collect

    out(0)(0).toString should equal("(2,[0],[1.0])")
    out(0)(1).toString should equal("(2,[1],[1.0])")

  }

  "#transformSchema" should "add columns" in {
    val testSchema = StructType(Seq(
      StructField("_1", DataTypes.DoubleType),
      StructField("_2", DataTypes.DoubleType)
    ))
    val pcv = new ParallelOneHotEncoder()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_ohe", "_2_ohe"))
    val outSchema = pcv.transformSchema(testSchema)

    val firstField = outSchema(2)
    firstField.dataType should equal(SQLDataTypes.VectorType)
    firstField.name should equal("_1_ohe")

    val secondField = outSchema(3)
    secondField.dataType should equal(SQLDataTypes.VectorType)
    secondField.name should equal("_2_ohe")
  }

}
