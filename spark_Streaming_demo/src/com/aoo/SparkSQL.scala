package com.aoo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object SparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("")
    val sc = new SparkContext(conf)
    val hive = new HiveContext(sc)
    val hjson = hive.jsonFile("")
    hjson.registerTempTable("stu")
    val sql = hive.sql("select * from stu")
    val result = sql.map { x => x(1)+"\t"+x(2) }
  }
}