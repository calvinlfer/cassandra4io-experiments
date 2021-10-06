package com.initialencoding

trait CassandraDataEncoder[A] {
  def encode(in: A): CassandraData
}

object CassandraDataEncoder {
  implicit val stringCDE: CassandraDataEncoder[String] =
    (in: String) => CassandraData.Primitive.CStr(in)

  implicit val intCDE: CassandraDataEncoder[Int] =
    (in: Int) => CassandraData.Primitive.CInt(in)

  implicit def listCDE[A](implicit cdeA: CassandraDataEncoder[A]): CassandraDataEncoder[List[A]] =
    (in: List[A]) => CassandraData.Collection.CList(in.map(cdeA.encode))

  implicit def setCDE[A](implicit cdeA: CassandraDataEncoder[A]): CassandraDataEncoder[Set[A]] =
    (in: Set[A]) => CassandraData.Collection.CSet(in.map(cdeA.encode))

  implicit def mapCDE[A, B](implicit
    cdeA: CassandraDataEncoder[A],
    cdeB: CassandraDataEncoder[B]
  ): CassandraDataEncoder[Map[A, B]] =
    (in: Map[A, B]) =>
      CassandraData.Collection.CMap(
        in.map { case (a, b) => (cdeA.encode(a), cdeB.encode(b)) }.toList
      )
}
