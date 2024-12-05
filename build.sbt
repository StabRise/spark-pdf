import xerial.sbt.Sonatype.sonatypeCentralHost
import xerial.sbt.Sonatype.GitHubHosting

ThisBuild / version := "0.1.12"

ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "com.stabrise"
ThisBuild / organizationName := "StabRise"
ThisBuild / organizationHomepage := Some(url("https://www.stabrise.com"))
ThisBuild / sonatypeProfileName := "com.stabrise"

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/StabRise/spark-pdf"),
    "scm:git@github.StabRise/spark-pdf.git"
  )
)

ThisBuild / sonatypeProjectHosting := Some(GitHubHosting("StabRise", "spark-pdf", "kolia1985@gmail.com"))
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / developers := List(
  Developer(
    id    = "kolia1985",
    name  = "Mykola Melnyk",
    email = "kolia1985@gmail.com",
    url   = url("https://stabrise.com")
  )
)

ThisBuild / description := "PDF Datasource for Apache Spark. Read PDF files to the DataFrame."
ThisBuild / licenses := List("AGPL-V3" -> new URL("https://www.gnu.org/licenses/agpl-3.0.html"))
ThisBuild / homepage := Some(url("https://stabrise.com/spark-pdf/"))
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
ThisBuild / sbtPluginPublishLegacyMavenStyle := false
ThisBuild / publishTo := sonatypePublishToBundle.value

root / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
root / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

val sparkVersion = System.getProperty("SPARK_VERSION", "3.5.0")


lazy val common = (project in file("common"))
  .settings(
    name := "common",
    commonSettings
  )
  .disablePlugins(AssemblyPlugin)


lazy val spark35 = (project in file("spark35"))
  .settings(
    name := "spark35",
    commonSettings,
  )
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common)

lazy val spark34 = (project in file("spark34"))
  .settings(
    name := "spark34",
    commonSettings,
  )
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common)

lazy val spark33 = (project in file("spark33"))
  .settings(
    name := "spark33",
    commonSettings,
  )
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common)

lazy val root = (project in file("."))
  .settings(
    name := "spark-pdf",
    commonSettings,
  )
  .dependsOn(dependencyModules():_*)
  .aggregate(aggregatedModules(): _*)

def aggregatedModules(): List[sbt.ProjectReference] =
  sparkVersion match {
    case sparkVersion if sparkVersion.startsWith("3.3") => List(common, spark33)
    case sparkVersion if sparkVersion.startsWith("3.4") => List(common, spark34)
    case _ =>  List(common, spark35)
  }
def dependencyModules(): List[ClasspathDependency] =
  sparkVersion match {
    case sparkVersion if sparkVersion.startsWith("3.3") => List(spark33).map(ClasspathDependency(_, None))
    case sparkVersion if sparkVersion.startsWith("3.4") => List(spark34).map(ClasspathDependency(_, None))
    case _ =>  List(spark35).map(ClasspathDependency(_, None))
  }

lazy val commonSettings = Seq(
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  libraryDependencies ++= commonDependencies
)

lazy val commonDependencies = Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
      "org.apache.pdfbox" % "pdfbox" % "2.0.24",
      "org.scalatest" %% "scalatest" % "3.2.9" % "test",
      "org.bytedeco" % "tesseract-platform" % "5.3.4-1.5.10",
      "org.bytedeco" % "tesseract" % "5.3.4-1.5.10",
      "net.sourceforge.tess4j" % "tess4j" % "5.11.0"
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("org.slf4j", "log4j-over-slf4j")
        exclude("org.slf4j", "jcl-over-slf4j")
        exclude("ch.qos.logback", "logback-classic")
        exclude("org.apache.pdfbox", "pdfbox"),
    ).map(_
      exclude("log4j", "log4j")
      exclude("org.apache.commons", "commons-csv")
      exclude("org.apache.commons", "commons-math3")
      exclude("commons-logging", "commons-logging")
      exclude("commons-cli", "commons-cli")
      exclude("org.junit.jupiter", "junit-jupiter")
)

lazy val assemblySettings = Seq(
  //assembly / assemblyOption := (assemblyOption in assembly).value.copy(includeScala = false),
  assembly / assemblyMergeStrategy := {
    case PathList("org", "xmlpull", xs@_*) => MergeStrategy.last
    case PathList("apache", "commons", "logging", "impl", xs@_*) => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains ( "android")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains ( "macos")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains ( "windows")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains ( "ios")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(p => p.contains("linux-arm") || p.contains("arm64-v8a") || p.contains("armeabi") ).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains("linux-ppc")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contentEquals("linux-x86")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contentEquals("windows-x86")).nonEmpty => MergeStrategy.discard
    case "versionchanges.txt" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains("pom.xml")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains("pom.properties")).nonEmpty => MergeStrategy.discard
    case "StaticLoggerBinder" => MergeStrategy.discard
    case PathList("net", "imglib2", "util", "StopWatch.class") => MergeStrategy.first
    case PathList("META-INF", fileName)
      if List("NOTICE", "MANIFEST.MF", "DEPENDENCIES", "INDEX.LIST").contains(fileName) || fileName.endsWith(".txt") || fileName.endsWith(".RSA") || fileName.endsWith(".DSA") || fileName.endsWith(".SF")
    => MergeStrategy.discard
    case "META-INF/services/javax.imageio.spi.ImageReaderSpi" => MergeStrategy.concat
    case PathList("META-INF", "services", _@_*) => MergeStrategy.first
    case PathList("META-INF", xs@_*) => MergeStrategy.first
    case PathList("plugins.config", xs@_*) => MergeStrategy.discard
    case PathList("LICENSE.txt",  xs@_*) => MergeStrategy.discard

    case PathList("org", "apache", "commons", "logging", xs@_*) => MergeStrategy.last
    case PathList("org", "apache", "batik", xs@_*) => MergeStrategy.last
    // case PathList("org", "bytedeco", "flycapture", "windows-x86_64", "jniFlyCapture2_C.dll") => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last contains ( ".DS_Store") => MergeStrategy.discard
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("net.imglib2.imglib2.util.**" -> "shadeio.imglib2.imglib2.util.@1").inAll,
    ShadeRule.rename("org.apache.http.**" -> "org.apache.httpShaded@1").inAll

  ),
  assembly / assemblyJarName := s"spark-pdf-${version.value}.jar",
  assembly / test  := {}
)
