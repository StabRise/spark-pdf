import xerial.sbt.Sonatype.sonatypeCentralHost
import xerial.sbt.Sonatype.GitHubHosting

ThisBuild / version := "0.1.17"

ThisBuild / scalaVersion := scala.util.Properties.envOrElse("SCALA_VERSION", "2.12.15") // "2.13.14", "2.12.15"
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

ThisBuild / sonatypeProjectHosting := Some(GitHubHosting("StabRise", "spark-pdf", "mykola.melnyk.ml@gmail.com"))
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / developers := List(
  Developer(
    id    = "mykolamelnykml",
    name  = "Mykola Melnyk",
    email = "mykola.melnyk.ml@gmail.com",
    url   = url("https://stabrise.com")
  )
)

ThisBuild / description := "Spark PDF a custom data source that enables efficient and scalable processing" +
  " of PDF files within the Apache Spark. Included OCR compatable with ScaleDP." +
  " Support for Apache Spark 3.3, 3.4, 3.5, 4.0."
ThisBuild / licenses := List("AGPL-V3" -> new URL("https://www.gnu.org/licenses/agpl-3.0.html"))
ThisBuild / homepage := Some(url("https://stabrise.com/spark-pdf/"))
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
ThisBuild / sbtPluginPublishLegacyMavenStyle := false
ThisBuild / publishTo := sonatypePublishToBundle.value

root / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
root / Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

// "4.0.0-preview2", "3.5.3","3.4.1", "3.3.2
val sparkVersion = scala.util.Properties.envOrElse("SPARK_VERSION", "3.5.3")

val packageName  =
  sparkVersion match {
    case sparkVersion if sparkVersion.startsWith("3.3") => "spark-pdf-spark33"
    case sparkVersion if sparkVersion.startsWith("3.4") => "spark-pdf-spark34"
    case sparkVersion if sparkVersion.startsWith("4.0") => "spark-pdf-spark40"
    case _ =>  "spark-pdf-spark35"
  }

val commonPackageName  =
  sparkVersion match {
    case sparkVersion if sparkVersion.startsWith("3.3") => "common-spark-pdf-spark33"
    case sparkVersion if sparkVersion.startsWith("3.4") => "common-spark-pdf-spark34"
    case sparkVersion if sparkVersion.startsWith("4.0") => "common-spark-pdf-spark40"
    case _ =>  "common-spark-pdf-spark35"
  }

lazy val common = (project in file("common"))
  .settings(
    name := commonPackageName,
    commonSettings,
    publish := { },
  )
  .disablePlugins(AssemblyPlugin)

lazy val spark40 = (project in file("spark40"))
  .settings(
    name := "spark40",
    commonSettings,
    publish := { },
  )
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common)

lazy val spark35 = (project in file("spark35"))
  .settings(
    name := "spark35",
    commonSettings,
    publish := { },
  )
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common)

lazy val spark34 = (project in file("spark34"))
  .settings(
    name := "spark34",
    commonSettings,
    publish := { },
  )
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common)

lazy val spark33 = (project in file("spark33"))
  .settings(
    name := "spark33",
    commonSettings,
    publish := { },
  )
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common)

lazy val root = (project in file("."))
  .settings(
    name := packageName,
    commonSettings,
    assemblySettings,
  )

  .dependsOn(dependencyModules():_*)
  .aggregate(aggregatedModules(): _*)

def aggregatedModules(): List[sbt.ProjectReference] =
  sparkVersion match {
    case sparkVersion if sparkVersion.startsWith("3.3") => List(common, spark33)
    case sparkVersion if sparkVersion.startsWith("3.4") => List(common, spark34)
    case sparkVersion if sparkVersion.startsWith("4.0") => List(common, spark40)
    case _ =>  List(common, spark35)
  }
def dependencyModules(): List[ClasspathDependency] =
  sparkVersion match {
    case sparkVersion if sparkVersion.startsWith("3.3") => List(spark33).map(ClasspathDependency(_, None))
    case sparkVersion if sparkVersion.startsWith("3.4") => List(spark34).map(ClasspathDependency(_, None))
    case sparkVersion if sparkVersion.startsWith("4.0") => List(spark40).map(ClasspathDependency(_, None))
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
  assembly / assemblyMergeStrategy := {
    case PathList("org", "xmlpull", xs@_*) => MergeStrategy.last
    case PathList("apache", "commons", "logging", "impl", xs@_*) => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains ( "android")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains ( "macos")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains ( "windows")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains ( "ios")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(p => p.contains("linux-arm") || p.contains("arm64-v8a") ||
      p.contains("armeabi") ).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains("linux-ppc")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contentEquals("linux-x86")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contentEquals("windows-x86")).nonEmpty => MergeStrategy.discard
    case "versionchanges.txt" => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains("pom.xml")).nonEmpty => MergeStrategy.discard
    case PathList(ps @ _*) if ps.filter(_.contains("pom.properties")).nonEmpty => MergeStrategy.discard
    case "StaticLoggerBinder" => MergeStrategy.discard
    case PathList("net", "imglib2", "util", "StopWatch.class") => MergeStrategy.first
    case PathList("META-INF", fileName)
      if List("NOTICE", "MANIFEST.MF", "DEPENDENCIES", "INDEX.LIST").contains(fileName) ||
        fileName.endsWith(".txt") || fileName.endsWith(".RSA") ||
        fileName.endsWith(".DSA") || fileName.endsWith(".SF")
    => MergeStrategy.discard
    case "META-INF/services/javax.imageio.spi.ImageReaderSpi" => MergeStrategy.concat
    case PathList("META-INF", "services", _@_*) => MergeStrategy.first
    case PathList("META-INF", xs@_*) => MergeStrategy.first
    case PathList("plugins.config", xs@_*) => MergeStrategy.discard
    case PathList("LICENSE.txt",  xs@_*) => MergeStrategy.discard
    case PathList("org", "apache", "commons", "logging", xs@_*) => MergeStrategy.last
    case PathList("org", "apache", "batik", xs@_*) => MergeStrategy.last
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


artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  //art.withClassifier(Some("assembly"))
  art
}

addArtifact(artifact in (Compile, assembly), assembly)
