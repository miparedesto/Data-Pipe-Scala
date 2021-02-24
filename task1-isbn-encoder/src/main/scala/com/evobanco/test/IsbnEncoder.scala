package com.evobanco.test.test

import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)
}

class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  /**
   * Creates a new row for each element of the ISBN code
   *
   * @return a data frame with new rows for each element of the ISBN code
   */
  def explodeIsbn(): DataFrame = {

    val emptyIsbn = getEmptyIsbn(df)
    val invalidIsbn = getInvalidIsbn(df)
    val explodeIsbn = getRegularIsbn(df)
    val finalDF = emptyIsbn union invalidIsbn union explodeIsbn

    finalDF
  }

  def getInvalidIsbn(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("isbn", when((length(col("isbn")) < 20) && (col("isbn") =!= ""), lit("INVALID-ISBN")).otherwise(col("isbn")))
      .filter(col("isbn") === "INVALID-ISBN")
  }

  def getEmptyIsbn(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("isbn", when((col("isbn")) === "", lit("")).otherwise(col("isbn")))
      .filter(col("isbn") === "")
  }

  def getRegularIsbn(dataFrame: DataFrame): DataFrame = {
    val isbnExplodePrepareDF = dataFrame
      .filter(col("isbn") =!= lit("") && (length(col("isbn")) === 20))
      .withColumn("isbn2", concat(substring(col("isbn"), 0, 20)))
      .withColumn("ean", concat(lit("ISBN-EAN: "), substring(col("isbn"), 7, 3)))
      .withColumn("group", concat(lit("ISBN-GROUP: "), substring(col("isbn"), 11, 2)))
      .withColumn("publisher", concat(lit("ISBN-PUBLISHER: "), substring(col("isbn"), 13, 4)))
      .withColumn("title", concat(lit("ISBN-TITLE: "), substring(col("isbn"), 17, 3)))
      .withColumn("isbn4",
        concat(
          col("isbn2"), lit("|"),
          col("ean"), lit("|"),
          col("group"), lit("|"),
          col("publisher"), lit("|"),
          col("title"), lit("|"),
          col("title"))
      )

    val isbnExplodeDF = isbnExplodePrepareDF
      .withColumn("isbn3", explode(array("isbn2", "ean", "group", "publisher", "title")))
      .select("name", "year", "isbn3")
      .withColumnRenamed("isbn3", "isbn")

    isbnExplodeDF
  }
}
