import xerial.sbt.Sonatype.sonatypeCentralHost
import xerial.sbt.Sonatype.GitHubHosting

ThisBuild / version := "0.1.7"

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
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true
ThisBuild / publishTo := sonatypePublishToBundle.value

val sparkVersion = "3.4.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark-pdf",
    idePackagePrefix := Some("com.stabrise.sparkpdf"),
    libraryDependencies ++= Seq(
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
      exclude("commons-cli", "commons-cli")),
  )
