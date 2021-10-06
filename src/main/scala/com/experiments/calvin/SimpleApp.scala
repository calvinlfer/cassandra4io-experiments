package com.experiments.calvin

import cats.effect._
import cats.syntax.all._
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._
import fs2._

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

final case class TableRow(a: NonNullable[Int], b: String, c: Double, d: Boolean)

object SimpleApp extends IOApp {
  class TableRepo(session: CassandraSession[IO]) {
    def put(row: TableRow): IO[Boolean] =
      cql"INSERT INTO example_table (a, b, c, d) VALUES (${row.a}, ${row.b}, ${row.c}, ${row.d})"
        .config(_.setConsistencyLevel(ConsistencyLevel.ONE))
        .execute(session)

    def get(a: Int): Stream[IO, TableRow] =
      cql"SELECT a, b, c, d FROM example_table WHERE a = $a"
        .as[TableRow]
        .select(session)
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val builder =
      CqlSession
        .builder()
        .addContactPoints(List(InetSocketAddress.createUnresolved("localhost", 9042)).asJava)
        .withLocalDatacenter("dc1")
        .withKeyspace("example")

    CassandraSession
      .connect[IO](builder)
      .map(new TableRepo(_))
      .use(_.get(1).compile.toList)
      .flatTap(rows => IO(println(rows)))
      .onError { case NonNullableConstraintViolated(m) =>
        IO(println(m))
      }
      .as(ExitCode.Success)
  }
}
