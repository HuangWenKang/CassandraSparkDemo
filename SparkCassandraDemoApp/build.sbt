name := "SparkCassandraDemoApp"

version := "1.0"

scalaVersion := "2.10.5"


libraryDependencies ++=Seq(
  "org.apache.spark"    %     "spark-core_2.10"                 %   "1.4.1",
  "com.datastax.spark"  %     "spark-cassandra-connector_2.10"  %   "1.4.0")


    