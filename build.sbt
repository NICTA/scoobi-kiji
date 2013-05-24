seq(sbtavro.SbtAvro.avroSettings: _*)

name := "scoobi-kiji"

organization := "com.nicta"

version := "0.1"

scalaVersion := "2.10.1"

scalacOptions ++= Seq("-deprecation", "-feature", "-language:_")

libraryDependencies ++= Seq(
  "com.nicta"              %% "scoobi"          % "0.7.0-cdh4-SNAPSHOT",
  "org.apache.commons"     %  "commons-math3"   % "3.1.1",
  "org.rogach"             %% "scallop"         % "0.8.0",
  "org.slf4j"              %  "slf4j-api"       % "1.7.5",
  "org.slf4j"              %  "slf4j-log4j12"   % "1.7.5",
  "log4j"                  %  "log4j"           % "1.2.17",
  "org.kiji.schema"        %  "kiji-schema"     % "1.0.3",
  "org.kiji.mapreduce"     %  "kiji-mapreduce"  % "1.0.0-rc62"       % "provided",
  "io.argonaut"            %% "argonaut"        % "6.0-M4",
  "joda-time"              %  "joda-time"       % "2.1",
  "org.joda"               %  "joda-convert"    % "1.1",
  "org.apache.hbase"       %  "hbase"           % "0.94.3",
  "cglib"                  %  "cglib-nodep"     % "2.2.2"            ,
  "org.easymock"           %  "easymock"        % "3.1"              ,
  "org.kiji.schema"        %  "kiji-schema"     % "1.0.3"            classifier "tests",
  "org.kiji.testing"       %  "fake-hbase_2.10" % "0.0.5"            ,
  "org.specs2"             %% "specs2"          % "2.0-RC1-SNAPSHOT"
)

resolvers ++= Seq(
  "sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "wibidata" at "https://repo.wibidata.com/artifactory/kiji"
)