package com.databricks.spark.sql.perf

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by youfuli on 7/22/16.
 */
object DataGen {
  import com.databricks.spark.sql.perf.tpcds.Tables
  val sparkConf = new SparkConf().setAppName("TPCDS-DG").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val dsdgenDir = "/home/youfuli/IdeaProjects/spark-sql-perf/src/main/tpcds_kit/tools"
  val scaleFactor = 1
  val tables = new Tables(sqlContext, dsdgenDir, scaleFactor)
  def gen_TPCDS_Data(): Unit ={
    tables.genData("/home/youfuli/IdeaProjects/datasets", "parquet", true, false, false, false, false)
  }
  def main (args: Array[String]){
    DataGen.gen_TPCDS_Data()
  }
}
