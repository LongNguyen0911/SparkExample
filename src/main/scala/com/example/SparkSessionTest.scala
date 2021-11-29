package com.example

import org.apache.spark.sql.SparkSession

object SparkSessionTest {
  case class Person(name: String, age: Long)
  def main(args:Array[String]): Unit ={
    println("Scala Version: "+scala.util.Properties.versionString)
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    println("First SparkContext:")
    println("APP Name :"+spark.sparkContext.appName)
    println("Deploy Mode :"+spark.sparkContext.deployMode)
    println("Master :"+spark.sparkContext.master)

    val rdd = spark.sparkContext.parallelize(List(Person("Long", 20), Person("Thanh", 18)))
    val rddCollect = rdd.collect()
    println("Number of Partitions: "+rdd.getNumPartitions)
    println("Action: First element: "+rdd.first())
    println("Action: RDD converted to Array[Person] : ")
    rddCollect.foreach(println)

    // Load Data From CSV to RDD

    val rddFromFile = spark.sparkContext.textFile("D:\\Projects\\Scala_Project\\First_Project\\StudentList.csv")

    val rdd2 = rddFromFile.map(f=>{
      f.split(",")
    })
    rdd2.foreach(f=>{
      println(s"Col1: ${f(0)} ,Col2: ${f(1)} ,Col3: ${f(2)} ,Col4: ${f(3)}")
    })

    val rdd3 = rddFromFile.flatMap(f => { f.split(",") })

    val filteredRDD1 = rdd3.filter(x => x.startsWith("Nguyen"))
    val filteredRDD2 = rdd3.filter(x => x.contains("Quynh"))

    val unionRDD = filteredRDD1.union(filteredRDD2)
    unionRDD.collect().foreach(println)
  }
}
