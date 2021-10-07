package com.userdefinedtypessupport

import cats.effect._
import com.datastax.oss.driver.api.core.CqlSession
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

import BinderExtras._

/**
 * CREATE TYPE nested_type(
 * nested frozen<set<frozen<set<basic_info>>>>
 * );
 */
final case class NestedType(nested: Set[Set[BasicInfo]])
object NestedType {
  implicit val nestedTypeBinder: Binder[NestedType] = UdtValueBinder.deriveTopLevel[NestedType]
  implicit val nestedTypeReads: Reads[NestedType]   = UdtValueReads.deriveTopLevel[NestedType]
}
final case class NestedExample(a: Int, b: NestedType)

object NestedUDTApp extends IOApp {
  val data = NestedExample(
    a = 1,
    b = NestedType(
      Set(
        Set(
          BasicInfo("weight1", Option(1), List(Datapoint(1, 2, Map(3 -> "three")))),
          BasicInfo("weight2", Option(2), List())
        )
      )
    )
  )

  override def run(args: List[String]): IO[ExitCode] =
    CassandraSession
      .connect[IO](
        CqlSession
          .builder()
          .addContactPoints(List(InetSocketAddress.createUnresolved("localhost", 9042)).asJava)
          .withLocalDatacenter("dc1")
          .withKeyspace("example")
      )
      .use { session =>
        cql"INSERT INTO nested_example (a, b) VALUES (${data.a}, ${data.b})"
          .execute(session) *>
          cql"SELECT a, b FROM nested_example WHERE a = ${data.a}"
            .as[NestedExample]
            .select(session)
            .evalTap(row => IO(println(row)))
            .compile
            .drain
            .as(ExitCode.Success)
      }
}
