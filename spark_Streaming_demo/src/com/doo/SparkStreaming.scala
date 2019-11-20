package com.doo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreaming {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local").setAppName("guoxiaoming")
    val sc = new StreamingContext(conf,Seconds(10))
    sc.checkpoint("d://");
    val topic = Array("hana").toSet
    val kafkaParam =Map[String,String]("metadata.borker.list"->"192.168.233.144:9092"); 
    val stream = KafkaUtils.createDirectStream(sc,kafkaParam,topic)
    stream.foreachRDD(rdd=>{
      rdd.foreach(line=>{
        println("key"+line._1+"value"+line._2)
      })
    })
    sc.start()
    sc.awaitTermination()
  }
}