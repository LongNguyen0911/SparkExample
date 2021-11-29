package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.lang

object BroadcastJoinExample {

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Broadcast Join Example")
    .getOrCreate()

  val big_table: Dataset[lang.Long] = spark.range(1, 100000000)

  val small_table_RDD: RDD[Row] = spark.sparkContext.parallelize(List(
    Row(1, "gold"),
    Row(2, "silver"),
    Row(3, "bronze")
  ))

  val small_table_schema: StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("medal", StringType)
  ))

  val small_table: DataFrame = spark.createDataFrame(small_table_RDD, small_table_schema)

  val joined: DataFrame = big_table.join(small_table, "id")
  joined.explain()
  //joined.show()

  /* Broadcast joining */
  val joinedBroadcast: DataFrame = big_table.join(broadcast(small_table), "id")
  joinedBroadcast.show()
}
