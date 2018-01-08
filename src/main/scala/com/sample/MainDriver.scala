package com.sample

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sinchan on 08/01/18.
  */
object MainDriver {

  case class logs(request_timestamp: String,
                  elb_name: String,
                  request_ip: String,
                  request_port: Int,
                  backend_ip: String,
                  backend_port: Int,
                  request_processing_time: String,
                  backend_processing_time: String,
                  client_response_time: String,
                  elb_response_code: String,
                  backend_response_code: String,
                  received_bytes: Long,
                  sent_bytes: Long,
                  request_verb: String,
                  url: String,
                  protocol: String,
                  user_agent: String,
                  ssl_cipher: String,
                  ssl_protocol: String)

  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      println("This program expects 1 argument, i.e. the log file path")
      System.exit(-1)
    }
    val master = args(0)
    val logFile = args(1)

    val convertTime = (request_timestamp:String) => {
      val date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").parse(request_timestamp)
      new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
    }

    val getIp = (complete_url:String) =>{
      complete_url.split(":")(0)
    }

    val getPort  = (complete_url:String) =>{
      val splitVals = complete_url.split(":")
      if(splitVals!=null && splitVals.length>1) splitVals(1)  else ""
    }

    val getFirst = (unsplitStr:String) => {
      unsplitStr.split(" ")(0)
    }

    val getSecond = (unsplitStr:String) => {
      unsplitStr.split(" ")(1)
    }

    val conf = new SparkConf()
    conf.setAppName("LogStatistics_Gen")
    conf.setMaster(master)
    val sparkConntext = new SparkContext(conf)
    val sqlContext = new HiveContext(sparkConntext)

    val rawlogDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("charset", "UTF8")
      .option("inferSchema", "true")
      .option("delimiter", " ")
      .load(logFile)
    rawlogDf.show()
    rawlogDf.registerTempTable("rawlogDf")

    sqlContext.udf.register("trunctomin",convertTime)
    sqlContext.udf.register("getIp",getIp)
    sqlContext.udf.register("getPort",getPort)
    sqlContext.udf.register("getFirst",getFirst)
    sqlContext.udf.register("getSecond",getSecond)

    val logDf = sqlContext.sql("select C0 as request_timestamp, C1 as elb_name, getIp(C2) as request_ip, getPort(C2) as request_port , getIp(C3) as  backend_ip, getPort(C3) as backend_port,   C4 as request_processing_time, C5 as backend_processing_time, C6 as client_response_time, C7 as elb_response_code, C8 as  backend_response_code, C9 as received_bytes, C10 as sent_bytes , getFirst(C11) as request_verb ,getSecond(C11) as url , getFirst(C12) as protocol , getSecond(C12) as user_agent , C13 as ssl_cipher, C14 as ssl_protocol from rawlogDf")
    //logDf.show()

    logDf.registerTempTable("logDf")

    val timebasedRespCodeBase = sqlContext.sql("select trunctomin(request_timestamp) as request_timestamp_min,elb_response_code,backend_response_code from logDf")
    //timebasedRespCodeBase.show()
    timebasedRespCodeBase.registerTempTable("timebasedRespCodeBase")

    val timebasedRespCode = sqlContext.sql("select request_timestamp_min,elb_response_code,backend_response_code,count(1) as code_occurance from timebasedRespCodeBase group by request_timestamp_min,elb_response_code,backend_response_code")
    timebasedRespCode.show()


    val returnSizeOfGets = sqlContext.sql("select sent_bytes,request_timestamp,elb_name,request_ip,request_port,url from logDf where request_verb like '%GET%' ")
    returnSizeOfGets.show()

    val ipWhoHitMultipleTimes = sqlContext.sql("select request_ip,count(1) as request_count from logDf group by request_ip").where("request_count > 1")
    ipWhoHitMultipleTimes.show()

    //write to hive tables
    timebasedRespCode.write.format("orc").saveAsTable("TimeResponseCodeTbl")
    returnSizeOfGets.write.format("orc").saveAsTable("GetSizeTbl")
    ipWhoHitMultipleTimes.write.format("orc").saveAsTable("IpMultiHitsTbl")

  }
}
