/*
 * Copyright (c) 2018 Machine Zone Inc. All rights reserved.
 */
package org.apache.spark.ml.feature.parallel

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * ParallelQuantileDiscretizerTest
  *
  * Unit tests for [[ParallelQuantileDiscretizer]].
  *
  * @author belbis
  * @since 1.0.0
  */
class ParallelQuantileDiscretizerTest extends TestBase {

  "#transform()" should "transform a dataframe" in {
    val data = Array((0, 18.0, 18.0), (1, 19.0, 19.0), (2, 8.0, 8.0), (3, 5.0, 5.0), (4, 2.2, 2.2))
    val df = spark.createDataFrame(data).toDF("_1", "_2", "_3")
    val discretizer = new ParallelQuantileDiscretizer()
      .setNumBuckets(2)
      .setRelativeError(0.0)
      .setInputCols(Array("_2", "_3"))
      .setOutputCols(Array("_2_qd", "_3_qd"))
    val result = discretizer.fit(df).transform(df)
    result.select("_2_qd", "_3_qd").show()
  }

  "#transformSchema()" should "transform a schema" in {
    val testSchema = StructType(Seq(
      StructField("_1", DataTypes.DoubleType),
      StructField("_2", DataTypes.DoubleType)
    ))

    val discretizer = new ParallelQuantileDiscretizer()
      .setInputCols(Array("_1", "_2"))
      .setOutputCols(Array("_1_qd", "_2_qd"))

    val outSchema = discretizer.transformSchema(testSchema)
    outSchema.fields.filter(_.name == "_1_qd").head.dataType should equal(DataTypes.DoubleType)
    outSchema.fields.filter(_.name == "_2_qd").head.dataType should equal(DataTypes.DoubleType)
  }

}
