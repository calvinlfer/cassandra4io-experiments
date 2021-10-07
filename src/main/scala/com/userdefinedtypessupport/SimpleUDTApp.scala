package com.userdefinedtypessupport

import cats.effect.{ExitCode, IO, IOApp}
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._
import fs2._

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

final case class Coordinate(x: Int, y: Int)
final case class NestedExampleUDT(d: Int, e: String, f: Set[String], g: Set[Coordinate])
final case class ExampleUDT(a: Int, b: Long, c: NestedExampleUDT)
object ExampleUDT {
  implicit val exampleUdtReads: Reads[ExampleUDT] =
    UdtValueReads.deriveTopLevel[ExampleUDT]

  implicit val exampleUdtBinder: Binder[ExampleUDT] =
    UdtValueBinder.deriveTopLevel[ExampleUDT]
}

final case class ExampleUDTTable(id: Int, udt: ExampleUDT)

object SimpleUDTApp extends IOApp {
  class ExampleRepo(session: CassandraSession[IO]) {
    def put(in: ExampleUDTTable): IO[Boolean] =
      cql"INSERT INTO example_udt_table (id, udt) VALUES (${in.id}, ${in.udt})"
        .execute(session)

    val get: Stream[IO, ExampleUDTTable] =
      cql"SELECT id, udt FROM example_udt_table"
        .as[ExampleUDTTable]
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
      .map(new ExampleRepo(_))
      .use(repo =>
        repo.put(
          ExampleUDTTable(
            id = 1,
            udt = ExampleUDT(
              a = 10,
              b = 100L,
              c = NestedExampleUDT(d = 1000, e = "10000", f = Set("one"), g = Set(Coordinate(1, 2)))
            )
          )
        ) *> repo.get
          .evalTap(row => IO(println(row)))
          .compile
          .drain
      )
      .as(ExitCode.Success)
  }
}
