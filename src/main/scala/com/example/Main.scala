package com.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkExercise")
    .getOrCreate()

  val products_table = spark.read.parquet("dataset/products_parquet")
  val sales_table = spark.read.parquet("dataset/sales_parquet")
  val sellers_table = spark.read.parquet("dataset/sellers_parquet")

  //                 Data Overview                       //

  products_table.printSchema()
  sales_table.printSchema()
  sellers_table.printSchema()

  println(s"Number of products: ${products_table.count()}"
         +s"\nNumber of sales: ${sales_table.count()}"
         +s"\nNumber of sellers: ${sellers_table.count()}")

  /*        Simple Query on Data            */
  // Number of products sold at least once //
  sales_table.agg(countDistinct(col("product_id"))).show()
  // Distinct products have been sold in each date //
  sales_table.groupBy(col("date")).agg(countDistinct(col("product_id")).alias("distinct_products_sold")).orderBy(
    col("distinct_products_sold").desc).show()
  //        Avg revenue of orders          //
  sales_table.join(products_table, "product_id").
    agg(avg(products_table("price")*sales_table("num_pieces_sold"))).show()
}
