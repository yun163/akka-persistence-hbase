organization := "com.coinport"

name := "akka-persistence-hbase"

version := "1.0.1-SNAPSHOT"

scalaVersion := "2.10.3"

val akkaVersion = "2.3.1"

resolvers += "coinport-repo" at "http://192.168.0.105:8081/nexus/content/groups/public"

resolvers += "maven2" at "http://repo1.maven.org/maven2"

libraryDependencies += "org.apache.hadoop" % "hadoop-core"   % "1.1.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.1.2"

libraryDependencies += "org.apache.hbase"  % "hbase"         % "0.94.6.1" % "compile"

libraryDependencies += ("org.hbase"        % "asynchbase"    % "1.4.1")
  .exclude("org.slf4j", "log4j-over-slf4j")
  .exclude("org.slf4j", "jcl-over-slf4j")

libraryDependencies += "org.slf4j"         % "slf4j-log4j12" % "1.6.0"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit"       % akkaVersion % "test"

libraryDependencies += "org.scalatest"     %% "scalatest"          % "2.0"       % "test"

parallelExecution in Test := false

// publishing settings

publishMavenStyle := true

publishArtifact in (Compile, packageDoc) := false

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishTo <<= (version) { version: String =>
  val nexus = "http://192.168.0.105:8081/nexus/content/repositories/"
  if (version.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "snapshots/")
  else                                   
    Some("releases"  at nexus + "releases/")
}

scalariformSettings