import Dependencies._

ThisBuild / scalaVersion     := "2.12.16"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.github.p2m2"
ThisBuild / organizationName := "p2m2"

val sparkVersion  = "3.1.3"
lazy val rdf4jVersion = "4.0.2"
lazy val slf4j_version = "1.7.36"

lazy val root = (project in file("."))
  .settings(
    name := "msd-metdisease-database-pmid-cid-builder",

		libraryDependencies ++= Seq(
      scalaTest % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion % "test,provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "test,provided",
      "com.lihaoyi" %% "requests" % "0.7.1",
      "com.github.scopt" %% "scopt" % "4.0.1",
      "org.slf4j" % "slf4j-simple" % slf4j_version,
      ("org.eclipse.rdf4j" % "rdf4j-sail" % rdf4jVersion).exclude("commons-codec","commons-codec"),
      ("org.eclipse.rdf4j" % "rdf4j-storage" % rdf4jVersion).exclude("commons-codec","commons-codec")
    ),
    resolvers ++= Seq(
      "AKSW Maven Releases" at "https://maven.aksw.org/archiva/repository/internal",
      "AKSW Maven Snapshots" at "https://maven.aksw.org/archiva/repository/snapshots",
      "oss-sonatype" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Apache repository (snapshots)" at "https://repository.apache.org/content/repositories/snapshots/",
      "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/", "NetBeans" at "https://bits.netbeans.org/nexus/content/groups/netbeans/", "gephi" at "https://raw.github.com/gephi/gephi/mvn-thirdparty-repo/",
      Resolver.defaultLocal,
      Resolver.mavenLocal,
      "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
      "Apache Staging" at "https://repository.apache.org/content/repositories/staging/"
    ),
    assembly / assemblyJarName := s"msd-metdisease-database-pmid-cid-builder.jar",
    assembly / logLevel := Level.Info,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "module-info.class"  => MergeStrategy.first
      case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

Global / onChangedBuildSource := ReloadOnSourceChanges

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
