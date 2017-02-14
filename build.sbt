name := "AffLinkAssociationRules"

version := "1.0"

scalaVersion := "2.11.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %%"spark-core"%"2.0.2",
  "org.apache.spark" %%"spark-mllib"%"2.0.2",
  "org.scalatest" %%"scalatest" %"3.0.0"
)
    