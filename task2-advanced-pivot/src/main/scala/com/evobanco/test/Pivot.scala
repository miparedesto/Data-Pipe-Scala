package com.evobanco.test

import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Pivot {
  val TypeMeasurement = "measurement"
  val TypeTest = "test"

  val TestPassed = "passed"
  val TestFailed = "failed"

  val AggFirst = "first"
  val AggLast = "last"

  val NameMeasurementPressure = "Pressure"
  val NameMeasurementFlow = "Flow"
  val NameTestVisual = "Visual test"
  val NameTestEOL = "EOL test"
  val NameTestFinal = "Final test"

  implicit def pivot(df: DataFrame) = new PivotImplicit(df)
}

class PivotImplicit(df: DataFrame) extends Serializable {

  /**
   * Pivots machine data
   *
   * @return machine data pivoted
   */

  def getTests(): DataFrame = {


    val measuresDF = df.filter((col("mType") === Pivot.TypeMeasurement))

    val orderMeasuresWindow = Window.partitionBy("part", "name").orderBy("unixTimestamp")

    val measuresFirstLastDF = measuresDF
      .withColumn("first", min(col("value")) over (orderMeasuresWindow))
      .withColumn("last", max(col("value")) over (orderMeasuresWindow))

    val measuresAggFirstDF = measuresFirstLastDF
      .withColumn("aggregation_mode", lit("first"))
      .withColumn("value", col("first"))

    val measuresAggLastDF = measuresFirstLastDF
      .withColumn("aggregation_mode", lit("last"))
      .withColumn("value", col("last"))

    val measuresUnionDF = measuresAggFirstDF union measuresAggLastDF

    val finalMeasuresUnionDF = measuresUnionDF
      .select("part", "name", "value", "aggregation_mode", "unixTimestamp")

    finalMeasuresUnionDF.show()

    val testDF = df.filter((col("mType") === Pivot.TypeTest))

    val orderTestWindow = Window.partitionBy("part", "name").orderBy("unixTimestamp")

    val testFirstLastDF = testDF
      .withColumn("first", min(col("testResult")) over (orderTestWindow))
      .withColumn("last", max(col("testResult")) over (orderTestWindow))

    val testAggFirstDF = testFirstLastDF
      .withColumn("aggregation_mode", lit("first"))
      .withColumn("testResult", col("first"))

    val testAggLastDF = testFirstLastDF
      .withColumn("aggregation_mode", lit("last"))
      .withColumn("testResult", col("last"))

    val testUnionDF = testAggFirstDF union testAggLastDF

    val finalTestUnionDF = testUnionDF
      .withColumnRenamed("name", "Test")
      .withColumnRenamed("testResult", "TestResult")
      .select("part", "Location", "Test", "TestResult", "aggregation_mode", "unixTimestamp")

    finalTestUnionDF.show()

    //Para cada Test(part test aggmode) me quedo el mas viejo
    val orderTestWindow2 = Window.partitionBy("part", "Test", "aggregation_mode").orderBy(desc("unixTimestamp"))
    val finalTestUnionDFMax = finalTestUnionDF
      .withColumn("older", row_number.over(orderTestWindow2))
      .filter(col("older")===1)
      .drop("older")


    //Para cada Measure(part,name, aggmode) me quedo el mas viejo
    val orderMeasureWindow2 = Window.partitionBy("part", "name", "aggregation_mode").orderBy(desc("unixTimestamp"))
    val finalMeasureUnionDFMax = finalMeasuresUnionDF
      .withColumn("older", row_number.over(orderMeasureWindow2))
      .filter(col("older")===1)
      .drop("older")

    val finalDF = finalTestUnionDFMax.join(finalMeasureUnionDFMax, Seq("part", "aggregation_mode"), "left")
    val selectFinalDF = finalDF
      .withColumn("features", array(array(s"name",s"value")))
      .withColumnRenamed("aggregation_mode", "Aggregation")
      .select("part", "Location", "Test", "TestResult", "Aggregation", "features")


    selectFinalDF.show()
    selectFinalDF

  }

}