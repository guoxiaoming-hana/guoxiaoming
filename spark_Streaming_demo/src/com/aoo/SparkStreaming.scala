package com.aoo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreaming {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local").setAppName("lll")
    var sc = new StreamingContext(conf,Seconds(10))
    sc.checkpoint("")
    var topic = Array("hana").toSet
    var kafkaMap = Map[String,String]("metadata.borker.list"->"192.168.233.114:9092")
    var stream = KafkaUtils.createDirectStream(sc,kafkaMap,topic);
    stream.foreachRDD(rdd=>{rdd.foreach(line=>{println("key"+line._1+"\tlalala"+line._2)})})
    /* stream.foreachRDD(rdd=>{
      rdd.foreach(line=>{
        println("key"+line._1+"value"+line._2)
      })
    })*/
  }
}