package com.viglink.associationrules

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alexminnaar on 1/25/17.
  */


object UrlExtensionExperiment extends App {

  val awsAccessKey = args(0)
  val awsSecretKey = args(1)

  import com.viglink.associationrules.Utils._

  //spark config
  val conf = new SparkConf()
    .setAppName("affiliate link association rules")
    .setMaster("local[3]")

  val sc = new SparkContext(conf)

  val rawData = sc.textFile(s"s3n://$awsAccessKey:$awsSecretKey@viglink-reports/account_custom/ad-hoc-analysis/alex-data/casas_experiment_2016-12-30_2017-01-29.tsv.gz")

  rawData.foreach{x=>

    val merchant_id = x.split('\t')(8)
    //println(merchant_id)

    if(merchant_id=="94077") {
      println(x)
    }
  }

  val conversions = rawData
    .map(row2Features)


  //conversions.foreach(x => println(x.toList))

  sc.stop()
}
