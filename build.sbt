name := "flink-jdbc-connector"

version := "1.0"

scalaVersion := "2.10.4"

unmanagedJars in Compile += file("/home/sangeeta/Desktop/flink-carbondata-1.3-SNAPSHOT-jar-with-dependencies.jar")
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.2.1"
libraryDependencies += "org.apache.flink" % "flink-jdbc" % "1.2.0"
libraryDependencies += "org.apache.flink" % "flink-java" % "1.2.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6"
libraryDependencies += "org.apache.flink" % "flink-clients_2.10" % "1.2.0"



