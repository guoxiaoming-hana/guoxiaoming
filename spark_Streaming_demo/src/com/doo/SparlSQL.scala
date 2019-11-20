package com.doo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.hive.HiveContext

object SparlSQL {
  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setMaster("local").setAppName("asd")
  val sc  =  new  SparkContext(conf)
  val hive = new HiveContext(sc)
  var rdd = hive.jsonFile("")
  rdd.registerTempTable("student")
  val resultSet = hive.sql("select * from student")
  val result = resultSet.map{row => row(1)+"\t"+row(2)}
  
    
  }
}