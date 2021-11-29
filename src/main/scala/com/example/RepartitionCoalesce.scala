package com.example

import org.apache.spark.sql.SparkSession

object RepartitionCoalesce {
  val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Repartition and Coalesce")
    .getOrCreate()

  val sc = spark.sparkContext

  val numbers = sc.parallelize(1 to 10000000) // 4 partitions

  val repartitionedNumbers = numbers.repartition(2)
  repartitionedNumbers.count()

  val coalescedNumbers = numbers.coalesce(2)
  coalescedNumbers.count()

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
