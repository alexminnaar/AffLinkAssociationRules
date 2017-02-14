package com.viglink.associationrules

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth


/**
  * Learn association rules on dataset that is result of Hive query
  *
  * The Hive query used is
  *
  * Select originaloutgoingurl,affiliatedurl, if(originaloutgoingurl=affiliatedurl,1,0), remoteipkey, browseragent, country, devicetype, network_id , merchant_id, if(revenue_based_click.sid is NULL,0,1) from
  * click
  * left outer join
  * member
  * on click.memberid = member.id
  * left outer join
  * merchant
  * on merchant.id = member.merchant_id
  * left outer join
  * revenue_based_click
  * on revenue_based_click.sid=click.sid
  * where (YEAR(click.dt)='2016' and MONTH(click.dt)='11' and (DAY(click.dt)='27' or DAY(click.dt)='28' or DAY(click.dt)='29')) and click.affstatus='A' and click.affprdt='ORG' and network_id!='171' and network_id!='172' and network_id!='173' and network_id!='174' and network_id!='175' and network_id!='176' and network_id!='50' and network_id!='86' and network_id!='192' and network_id!='193' and network_id!='51' and network_id!='71'
  */
object LinkAssociationRules extends App {

  val awsAccessKey = args(0)
  val awsSecretKey = args(1)

  import com.viglink.associationrules.Utils._

  //spark config
  val conf = new SparkConf()
    .setAppName("affiliate link association rules")
    .setMaster("local[3]")

  val sc = new SparkContext(conf)

  //val rawData = sc.textFile("s3n://AKIAINQKH5XJEVTMGUGQ:fYQN0IKWoozl6Ro3CN6AzdA752efSS1DM2XFmrvP@viglink-reports/account_custom/ad-hoc-analysis/alex-data/sameUrlsExperiment_2016-11-01_2016-12-01.tsv.gz")
  //val rawData = sc.textFile("s3n://AKIAINQKH5XJEVTMGUGQ:fYQN0IKWoozl6Ro3CN6AzdA752efSS1DM2XFmrvP@viglink-reports/account_custom/ad-hoc-analysis/alex-data/sameUrlsExperiment_2016-11-01_2016-12-01.tsv.gz")
  //val rawData = sc.textFile(s"s3n://$awsAccessKey:$awsSecretKey@viglink-reports/account_custom/ad-hoc-analysis/alex-data/black_friday_click_2016-12-09_2017-01-08.tsv.gz")
  val rawData = sc.textFile(s"s3n://$awsAccessKey:$awsSecretKey@viglink-reports/account_custom/ad-hoc-analysis/alex-data/casas_experiment_2016-12-27_2017-01-26.tsv.gz")


  //rawData.foreach(println)

  val conversions = rawData
    .map(row2Features)
    .filter(validityFilter)

  val fpg = new FPGrowth()
    .setMinSupport(0.01)

  val model = fpg.run(conversions)

  val minConfidence = 0.998

  //only interested in association rules where the consequence is no conversion
  val nonConversionRules = model
    .generateAssociationRules(minConfidence)
    .collect()
    .filter(rule => rule.consequent.mkString("[", ",", "]").contains("no_conversion") && (rule.antecedent.size <3))
    .map {
      x => (x.antecedent.mkString("[", ",", "]"), x.confidence)
    }

  //sort rules by confidence
  val sortedRules = nonConversionRules.sortBy(-_._2)

  sortedRules.foreach(println)

//  println(conversions.filter(x => x.last == "conversion").count())
  println(conversions.count())

  sc.stop()
}
