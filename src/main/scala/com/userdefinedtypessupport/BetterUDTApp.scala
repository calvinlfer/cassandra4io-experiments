package com.userdefinedtypessupport

import cats.effect.{ExitCode, IO, IOApp}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._
import scala.jdk.CollectionConverters._
import java.net.InetSocketAddress
import BinderExtras._

/**
 * CREATE TYPE datapoint (
 *   x int,
 *   y int,
 *   z frozen<map<int, text>>
 * );
 */
final case class Datapoint(x: Int, y: Int, z: Map[Int, String])

/**
 * CREATE TYPE basic_info (
 *    weight text,
 *    height double
 *    datapoints frozen<list<datapoint>>
 * );
 */
final case class BasicInfo(weight: String, height: Option[Double], datapoints: List[Datapoint])

/**
 * CREATE TABLE IF NOT EXISTS embedded_udt_set_example(
 * id int,
 * info frozen<set<basic_info>>,
 * PRIMARY KEY (id)
 * );
 */
final case class YetAnotherExampleRow(a: Int, info: Set[BasicInfo])

object BetterUDTApp extends IOApp {
  class ExampleRepo(session: CassandraSession[IO]) {
    def put(row: YetAnotherExampleRow): IO[Boolean] =
      cql"INSERT INTO embedded_udt_set_example (id, info) VALUES (${row.a}, ${row.info})"
        .config(_.setConsistencyLevel(ConsistencyLevel.ONE))
        .execute(session)
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
//        repo.put2(
//          NestedExample(
//            1,
//            NestedType(
//              Set(
//                Set(
//                  BasicInfo(
//                    weight = "tall",
//                    height = Option(100.0),
//                    datapoints = List(
//                      Datapoint(1, 2, Map(3 -> "three")),
//                      Datapoint(4, 5, Map(6 -> "six", 7 -> "seven"))
//                    )
//                  ),
//                  BasicInfo(
//                    weight = "med",
//                    height = Option(80.0),
//                    datapoints = List(
//                      Datapoint(6, 7, Map.empty)
//                    )
//                  )
//                )
//              )
//            )
//          )
//        )
        repo.put(
          YetAnotherExampleRow(
            99,
            Set(
              BasicInfo(
                weight = "tall",
                height = Option(100.0),
                datapoints = List(
                  Datapoint(1, 2, Map(3 -> "three")),
                  Datapoint(4, 5, Map(6 -> "six", 7 -> "seven"))
                )
              ),
              BasicInfo(
                weight = "med",
                height = None,
                datapoints = List(
                  Datapoint(10, 11, Map(12 -> "twelve"))
                )
              )
            )
          )
        )
      )
      .as(ExitCode.Success)
  }
}
