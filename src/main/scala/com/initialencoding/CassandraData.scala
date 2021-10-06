package com.initialencoding

sealed trait CassandraData
object CassandraData {
  sealed trait Primitive extends CassandraData
  object Primitive {
    // TODO: add missing types supported by Cassandra
    final case class CStr(value: String) extends Primitive
    final case class CInt(value: Int)    extends Primitive
    final case class CDbl(value: Double) extends Primitive
    final case class CLng(value: Long)   extends Primitive
  }

  sealed trait Collection extends CassandraData
  object Collection {
    final case class CList(values: List[CassandraData])                extends Collection
    final case class CSet(values: Set[CassandraData])                  extends Collection
    final case class CMap(pairs: List[(CassandraData, CassandraData)]) extends Collection
  }

  final case class CUDT(pairs: List[LabelledData]) extends CassandraData
}

final case class LabelledData(label: String, value: CassandraData)

final case class CassandraDataMeta(data: CassandraData)
