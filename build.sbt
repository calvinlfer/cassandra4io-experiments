name := "cassandra4io-test"

version := "0.1"

scalaVersion := "2.13.6"
scalacOptions ++= Seq("-Vimplicits", "-Vtype-diffs")

libraryDependencies ++= {
  Seq(
    "com.ringcentral" %% "cassandra4io" % "0.1.6",
    "dev.zio"         %% "zio-prelude"  % "1.0.0-RC6"
  )
}
