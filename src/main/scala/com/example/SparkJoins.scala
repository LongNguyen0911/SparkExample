package com.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object SparkJoins {

  val spark: SparkSession = SparkSession.builder()
    .appName("ALL THE JOINS")
    .master("local")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val kids: RDD[Row] = sc.parallelize(List(
    Row(40, "Mary", 1),
    Row(41, "Jane", 3),
    Row(42, "David", 3),
    Row(43, "Angela", 2),
    Row(44, "Charlie", 1),
    Row(45, "Jimmy", 2),
    Row(46, "Lonely", 7)
  ))

  val kidsSchema: StructType = StructType(Array(
    StructField("Id", IntegerType),
    StructField("Name", StringType),
    StructField("Team", IntegerType)
  ))

  val kidsDF: DataFrame = spark.createDataFrame(kids, kidsSchema)

  val teams: RDD[Row] = sc.parallelize(List(
    Row(1, "The Invincibles"),
    Row(2, "Dog Lovers"),
    Row(3, "Rockstars"),
    Row(4, "The Non-Existent Team")
  ))

  val teamsSchema: StructType = StructType(Array(
    StructField("TeamId", IntegerType),
    StructField("TeamName", StringType)
  ))

  val teamsDF: DataFrame = spark.createDataFrame(teams, teamsSchema)

  //  inner joins
  val joinCondition: Column = kidsDF.col("Team") === teamsDF.col("TeamId")
  val kidsTeamsDF: DataFrame = kidsDF.join(teamsDF, joinCondition, "inner")
  //kidsTeamsDF.show()

  //  outer joins
  val allKidsTeamsDF: DataFrame = kidsDF.join(teamsDF, joinCondition, "left_outer")
  //allKidsTeamsDF.show()
  val allTeamsKidsDF: DataFrame = kidsDF.join(teamsDF, joinCondition, "right_outer")
  //allTeamsKidsDF.show()
  val fullTeamsKidsDF: DataFrame = kidsDF.join(teamsDF, joinCondition, "full_outer")
  //fullTeamsKidsDF.show()

  //  spark semi joins
  val allKidsWithTeamsDF: DataFrame = kidsDF.join(teamsDF, joinCondition, "left_semi")
  //allKidsWithTeamsDF.show()

  //  spark anti joins
  val kidsWithNoTeamsDF: DataFrame = kidsDF.join(teamsDF, joinCondition, "left_anti")
  //kidsWithNoTeamsDF.show()

  //  spark cross joins
  val productKidsWithTeams: DataFrame = kidsDF.crossJoin(teamsDF)
  //productKidsWithTeams.show()

  def main(args: Array[String]): Unit = {

  }

}
