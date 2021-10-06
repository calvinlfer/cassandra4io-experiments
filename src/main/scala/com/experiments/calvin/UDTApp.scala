package com.experiments.calvin

import cats.effect._
import cats.syntax.all._
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._
import fs2._

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

final case class AnotherExampleRow(a: Int, info: BasicInfo)

object UDTApp extends IOApp {
  class AnotherExampleRepo(session: CassandraSession[IO]) {
    def create =
      cql"""CREATE TABLE IF NOT EXISTS another_example(
           | id int,
           | info basic_info
           |)""".stripMargin

    def put(row: AnotherExampleRow): IO[Boolean] =
      cql"INSERT INTO another_example (id, info) VALUES (${row.a}, ${row.info})"
        .config(_.setConsistencyLevel(ConsistencyLevel.ONE))
        .execute(session)

    def get(id: Int): Stream[IO, AnotherExampleRow] =
      cql"SELECT id, info FROM another_example WHERE id = $id"
        .as[AnotherExampleRow]
        .select(session)
  }

  /**
   * CREATE KEYSPACE example WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
   * CREATE TYPE basic_info (
   *  weight text,
   *  height double
   * );
   * @param args
   * @return
   */
  override def run(args: List[String]): IO[ExitCode] = {
    val builder =
      CqlSession
        .builder()
        .addContactPoints(List(InetSocketAddress.createUnresolved("localhost", 9042)).asJava)
        .withLocalDatacenter("dc1")
        .withKeyspace("example")

    CassandraSession
      .connect[IO](builder)
      .map(new AnotherExampleRepo(_))
      .use(repo => repo.put(AnotherExampleRow(5, BasicInfo(100.0, "tall"))) *> repo.get(5).compile.toList)
      .flatTap(rows => IO(println(rows)))
      .as(ExitCode.Success)
  }
}
