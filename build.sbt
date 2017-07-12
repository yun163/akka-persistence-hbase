organization := "com.coinport"

name := "akka-persistence-hbase"

version := "1.0.15-SNAPSHOT"

scalaVersion := "2.10.4"

val akkaVersion = "2.3.3"

// resolvers += "coinport-repo" at "https://nexus.coinport.com/nexus/content/groups/public"

resolvers += "maven2" at "http://repo1.maven.org/maven2"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-hdfs-nfs" % "2.4.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0",
  "commons-configuration" % "commons-configuration" % "1.6",
  "org.apache.hadoop" % "hadoop-common" % "2.4.0",
  "org.apache.hadoop" % "hadoop-yarn-client" % "2.4.0",
  "org.apache.hbase" % "hbase-client" % "1.3.1" % "compile",
  "org.apache.hbase" % "hbase-common" % "1.3.1" % "compile",
  "org.slf4j" % "slf4j-log4j12" % "1.6.0",
  "com.twitter" %% "util-eval" % "6.12.1",
  "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion % "compile",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "2.0" % "test")

libraryDependencies += ("org.hbase"        % "asynchbase"    % "1.8.0")
  .exclude("org.slf4j", "log4j-over-slf4j")
  .exclude("org.slf4j", "jcl-over-slf4j")

parallelExecution in Test := false

// publishing settings

publishMavenStyle := true

publishArtifact in (Compile, packageDoc) := false

publishArtifact in (Compile, packageSrc) := false

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo <<= (version) {
  version: String =>
    val nexus = "https://nexus.coinport.com/nexus/content/repositories/"
      if (version.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "snapshots/")
      else
        Some("releases"  at nexus + "releases/")
}

scalariformSettings
