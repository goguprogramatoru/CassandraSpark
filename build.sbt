name := "CassandraSpark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.0-M2"

libraryDependencies += "org.apache.spark"  %% "spark-sql" % "1.6.1"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.2"
