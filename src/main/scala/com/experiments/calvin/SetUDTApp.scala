package com.experiments.calvin

import cats.effect._
import cats.syntax.all._
import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Row}
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.internal.core.`type`.DefaultSetType
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._
import fs2._
import shapeless.labelled.FieldType
import shapeless.{HList, Lazy, Witness}

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

object UserDefinedTypeValue {
  implicit val userDefinedTypeValueReads: Reads[UdtValue] =
    (row: Row, index: Int) => (row.getUdtValue(index), index + 1)

  implicit val userDefinedTypeValueBinder: Binder[UdtValue] =
    (statement: BoundStatement, index: Int, value: UdtValue) => (statement.setUdtValue(index, value), index + 1)

  implicit class UserDefinedTypeValueReadsOps(udtReads: Reads[UdtValue]) {
    def map[B](f: UdtValue => B): Reads[B] = (row: Row, index: Int) => {
      val (underlying, nextIndex) = udtReads.read(row, index)
      (f(underlying), nextIndex)
    }
  }
}
//
//  implicit class UserDefinedTypeValueBinderOps(udtBinder: Binder[UdtValue]) {
//    def contramapWithSchema[A](f: (A, UserDefinedType) => UdtValue): Binder[A] = new Binder[A] {
//      override def bind(statement: BoundStatement, index: Int, value: A): (BoundStatement, Int) = {
//        val udtValue = f(
//          value,
//          statement.getPreparedStatement.getVariableDefinitions.get(index).getType.asInstanceOf[UserDefinedType]
//        )
//        udtBinder.bind(statement, index, udtValue)
//      }
//    }
//  }
//
//  implicit class UserDefinedTypeValueNestedBinderOps[F[_]](udtBinder: Binder[F[UdtValue]]) {
//    def contramapNestedUDT[A](f: (F[A], UserDefinedType) => F[UdtValue]): Binder[F[A]] = new Binder[F[A]] {
//      override def bind(statement: BoundStatement, index: Int, value: F[A]): (BoundStatement, Int) = {
//        val udtValue = f(
//          value,
//          statement.getPreparedStatement.getVariableDefinitions.get(index).getType.asInstanceOf[UserDefinedType]
//        )
//        udtBinder.bind(statement, index, udtValue)
//      }
//    }
//  }
//}

//object SetFunc {
//  implicit def setBinder[A](implicit tag: ClassTag[A]): Binder[Set[A]] =
//    new Binder[Set[A]] {
//      override def bind(statement: BoundStatement, index: Int, value: Set[A]): (BoundStatement, Int) =
//        (statement.setSet(index, value.asJava, tag.runtimeClass.asInstanceOf[Class[A]]), index + 1)
//    }
//
//  implicit def setReads[A](implicit tag: ClassTag[A]): Reads[Set[A]] =
//    (row: Row, index: Int) => (row.getSet(index, tag.runtimeClass.asInstanceOf[Class[A]]).asScala.toSet, index + 1)
//}

final case class BasicInfo(height: Double, weight: String)
object BasicInfo {
  import UserDefinedTypeValue._

  implicit val basicInfoCqlReads: Reads[BasicInfo] =
    Reads[UdtValue].map { udt =>
      val height = udt.getDouble("height")
      val weight = udt.getString("weight")
      BasicInfo(height, weight)
    }

  implicit val basicInfoBinder: Binder[BasicInfo] =
    UdtValueBinder.deriveBinder[BasicInfo]
}

final case class YetAnotherExampleRow(a: Int, info: Set[BasicInfo])
object YetAnotherExampleRow {
  implicit val binderSetBasicInfo: Binder[Set[BasicInfo]] =
    new Binder[Set[BasicInfo]] {
      override def bind(statement: BoundStatement, index: Int, value: Set[BasicInfo]): (BoundStatement, Int) = {
        val userDefinedType =
          statement.getPreparedStatement.getVariableDefinitions
            .get(index)
            .getType
            .asInstanceOf[DefaultSetType]
            .getElementType
            .asInstanceOf[UserDefinedType]
        (
          statement
            .setSet(
              index,
              value
                .map(info =>
                  userDefinedType
                    .newValue()
                    .setString("weight", info.weight)
                    .setDouble("height", info.height)
                )
                .asJava,
              classOf[UdtValue]
            ),
          index + 1
        )
      }
    }

  implicit val readsSetBasicInfo: Reads[Set[BasicInfo]] =
    new Reads[Set[BasicInfo]] {
      override def read(row: Row, index: Int): (Set[BasicInfo], Int) =
        (
          row
            .getSet[UdtValue](index, classOf[UdtValue])
            .asScala
            .map(udt => BasicInfo(height = udt.getDouble("height"), weight = udt.getString("weight")))
            .toSet,
          index + 1
        )
    }
}

object SetUDTApp extends IOApp {
  import YetAnotherExampleRow._

  class AnotherExampleRepo(session: CassandraSession[IO]) {
    def put(row: YetAnotherExampleRow): IO[Boolean] =
      cql"INSERT INTO yet_another_example (id, info) VALUES (${row.a}, ${row.info})"
        .config(_.setConsistencyLevel(ConsistencyLevel.ONE))
        .execute(session)

    def get(id: Int): Stream[IO, YetAnotherExampleRow] =
      cql"SELECT id, info FROM yet_another_example WHERE id = $id"
        .as[YetAnotherExampleRow]
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
      .use(repo =>
        repo.put(YetAnotherExampleRow(2, Set(BasicInfo(100.0, "tall")))) *> repo
          .get(5)
          .compile
          .toList
      )
      .flatTap(rows => IO(println(rows)))
      .as(ExitCode.Success)
  }
}
