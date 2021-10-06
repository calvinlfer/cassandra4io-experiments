package com.userdefinedtypessupport

import cats.effect._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{DefaultListType, DefaultSetType}
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

object ManualUDTApp extends IOApp {

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
  final case class BasicInfo(weight: String, height: Double, datapoints: List[Datapoint])

  /**
   * CREATE TABLE IF NOT EXISTS embedded_udt_set_example(
   * id int,
   * info frozen<set<basic_info>>,
   * PRIMARY KEY (id)
   * );
   */
  final case class YetAnotherExampleRow(a: Int, info: Set[BasicInfo])
  object YetAnotherExampleRow {
    implicit val binderSetBasicInfo: Binder[Set[BasicInfo]] = {
      (statement: BoundStatement, index: Int, value: Set[BasicInfo]) =>
        val basicInfoUdtConstructor =
          statement.getPreparedStatement.getVariableDefinitions
            .get(index)
            .getType
            .asInstanceOf[DefaultSetType] // We know its a Set
            .getElementType
            .asInstanceOf[UserDefinedType] // We know its a UserDefinedType inside the Set

        // BasicInfo UDT
        val basicInfoSet =
          value.map { basicInfo =>
            val constructor = basicInfoUdtConstructor.newValue()
            val datapointConstructor =
              constructor
                .getType("datapoints")
                .asInstanceOf[DefaultListType]
                .getElementType
                .asInstanceOf[UserDefinedType] // We know its a UserDefinedType inside the Set

            val datapointsUdt =
              basicInfo.datapoints
                .map(datapoint =>
                  datapointConstructor
                    .newValue()
                    .setInt("x", datapoint.x)
                    .setInt("y", datapoint.y)
                    .setMap[java.lang.Integer, String](
                      "z",
                      datapoint.z.map { case (k, v) => (Int.box(k), v) }.asJava,
                      classOf[java.lang.Integer],
                      classOf[String]
                    )
                )
                .asJava

            constructor
              .setString("weight", basicInfo.weight)
              .setDouble("height", basicInfo.height)
              .setList[UdtValue](
                "datapoints",
                datapointsUdt,
                classOf[UdtValue]
              )
          }.asJava

        (statement.setSet[UdtValue](index, basicInfoSet, classOf[UdtValue]), index + 1)
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {
    import YetAnotherExampleRow._

    val builder =
      CqlSession
        .builder()
        .addContactPoints(List(InetSocketAddress.createUnresolved("localhost", 9042)).asJava)
        .withLocalDatacenter("dc1")
        .withKeyspace("example")

    val data =
      YetAnotherExampleRow(
        a = 100,
        info = Set(BasicInfo(weight = "heavy", height = 1000, datapoints = List(Datapoint(1, 2, Map(3 -> "three")))))
      )

    CassandraSession
      .connect[IO](builder)
      .use { session =>
        cql"INSERT INTO embedded_udt_set_example (id, info) VALUES (${data.a}, ${data.info})".execute(session)
      }
      .as(ExitCode.Success)
  }
}
