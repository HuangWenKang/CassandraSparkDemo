name := "SparkCassandraDemoApp"

version := "1.0"

scalaVersion := "2.10.5"

//http://stackoverflow.com/questions/32413887/use-spark-in-a-sbt-project-in-intellij
//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1"


libraryDependencies ++=Seq(
  "org.apache.spark"    %     "spark-core_2.10"                 %   "1.4.1",
  "com.datastax.spark"  %     "spark-cassandra-connector_2.10"  %   "1.4.0")


    