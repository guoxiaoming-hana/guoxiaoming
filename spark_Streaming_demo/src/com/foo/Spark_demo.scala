package com.foo


import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.SparkConf


/**
  * spark整合kafka
  */
object Spark_demo {
  def main(args: Array[String]):Unit= {
   // val spark = SparkSession.builder().appName("guoxiaoming123").master("local[*]").getOrCreate()
   var conf =  new SparkConf
   conf.setAppName("guoxiaoming123")
   conf.setMaster("local[*]")
  var streamContext =  new StreamingContext(conf,Seconds(10))
    streamContext.checkpoint("d:/checkpoint")
    val topics = Array("guoxiaoming").toSet
    val kafKaParams = Map[String,String]("metadata.broker.list"->"192.168.233.100:9092")
    val stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](streamContext,kafKaParams,topics)
    stream.foreachRDD(rdd=>{
      rdd.foreach(line=>{
        println("key="+ line._1 +"  value="+ line._2 )
      }
      )
    }
    )
    streamContext.start() 
    streamContext.awaitTermination()
  }


}