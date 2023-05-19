name := "push-with-grpc-sample"

version := "1.0"

scalaVersion := "2.13.8"

lazy val akkaVersion = "2.6.20"
lazy val akkaHttpVersion = "10.2.10"
lazy val akkaGrpcVersion = "2.1.6"

enablePlugins(AkkaGrpcPlugin)

fork := true

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http2-support" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
  "com.typesafe.akka" %% "akka-pki" % akkaVersion,

  // The Akka HTTP overwrites are required because Akka-gRPC depends on 10.1.x
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http2-support" % akkaHttpVersion,

  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.1.1" % Test
)
