package com.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RepartitionCoalesce {
  val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Repartition and Coalesce")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val numbers: RDD[Int] = sc.parallelize(1 to 10000000) // 4 partitions

  val repartitionedNumbers: RDD[Int] = numbers.repartition(2)
  repartitionedNumbers.count()

  val coalescedNumbers: RDD[Int] = numbers.coalesce(2)
  coalescedNumbers.count()

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
