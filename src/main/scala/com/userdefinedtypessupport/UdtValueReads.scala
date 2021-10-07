package com.userdefinedtypessupport

import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue
import com.ringcentral.cassandra4io.cql.Reads

import scala.jdk.CollectionConverters._

trait UdtValueReads[A] {
  def isObject: Boolean = false
  def read(fieldName: String, in: UdtValue): A
}

object UdtValueReads extends LowerPriorityUdtValueReads with LowestPriorityUdtValueReads {
  trait Object[A] extends UdtValueReads[A] {
    override def isObject: Boolean = true
  }

  def deriveTopLevel[A](implicit ev: UdtValueReads.Object[A]): Reads[A] = { (row: Row, index: Int) =>
    (ev.read("unused", row.getUdtValue(index)), index + 1)
  }

  def apply[A](implicit e: UdtValueReads.Object[A]): UdtValueReads.Object[A] = e

  implicit val intUdtValueReads: UdtValueReads[Int] =
    (fieldName: String, in: UdtValue) => in.getInt(fieldName)

  implicit val dblUdtValueReads: UdtValueReads[Double] =
    (fieldName: String, in: UdtValue) => in.getDouble(fieldName)

  implicit val lngUdtValueReads: UdtValueReads[Long] =
    (fieldName: String, in: UdtValue) => in.getLong(fieldName)

  implicit val stringUdtValueReads: UdtValueReads[String] =
    (fieldName: String, in: UdtValue) => in.getString(fieldName)

  implicit val shortUdtValueReads: UdtValueReads[Short] =
    (fieldName: String, in: UdtValue) => in.getShort(fieldName)

  implicit val booleanUdtValueReads: UdtValueReads[Boolean] =
    (fieldName: String, in: UdtValue) => in.getBoolean(fieldName)

  implicit val bigIntUdtValueReads: UdtValueReads[BigInt] = (fieldName: String, in: UdtValue) =>
    in.getBigInteger(fieldName)

  implicit val bigDecimalUdtValueReads: UdtValueReads[BigDecimal] =
    (fieldName: String, in: UdtValue) => in.getBigDecimal(fieldName)

  implicit def nestedOneLevelSetPrimValueReads[A](implicit
    ev: CassPrimitiveType[A]
  ): UdtValueReads[Set[Set[A]]] = { (fieldName: String, in: UdtValue) =>
    val typeToken = new GenericType[java.util.Set[java.util.Set[ev.CassType]]] {}
    in.get(fieldName, typeToken)
      .asScala
      .map(set => set.asScala.map(ev.fromCassandra(_)).toSet)
      .toSet
  }

  implicit def nestedOneLevelSetUdtValueReads[A](implicit
    ev: UdtValueReads[A]
  ): UdtValueReads[Set[Set[A]]] = { (fieldName: String, in: UdtValue) =>
    val typeToken = new GenericType[java.util.Set[java.util.Set[UdtValue]]] {}
    in.get(fieldName, typeToken)
      .asScala
      .map(set => set.asScala.map(ev.read("unused", _)).toSet)
      .toSet
  }
}

trait LowerPriorityUdtValueReads {
  // For Option[CaseClass/UdtValue]
  implicit def optionUdtValueReads[A](implicit udtValueReads: UdtValueReads.Object[A]): UdtValueReads[Option[A]] =
    (fieldName: String, in: UdtValue) => {
      val raw = in.getUdtValue(fieldName)
      if (raw == null) None
      else Some(udtValueReads.read("unused", raw))
    }

  // For Option[Cassandra Primitive - Int/String/Long]
  implicit def optionPrimValueReads[A](implicit prim: CassPrimitiveType[A]): UdtValueReads[Option[A]] =
    (fieldName: String, in: UdtValue) => {
      val raw = in.get[prim.CassType](fieldName, prim.cassType)
      if (raw == null) None
      else Some(prim.fromCassandra(raw))
    }

  implicit def setPrimValueReads[A](implicit ev: CassPrimitiveType[A]): UdtValueReads[Set[A]] =
    (fieldName: String, in: UdtValue) =>
      in.getSet[ev.CassType](fieldName, ev.cassType)
        .asScala
        .map(ev.fromCassandra)
        .toSet

  implicit def setUdtValueReads[A](implicit ev: UdtValueReads.Object[A]): UdtValueReads[Set[A]] =
    (fieldName: String, in: UdtValue) =>
      in.getSet[UdtValue](fieldName, classOf[UdtValue])
        .asScala
        .map(ev.read("unused", _))
        .toSet

  implicit def listPrimValueReads[A](implicit ev: CassPrimitiveType[A]): UdtValueReads[List[A]] =
    (fieldName: String, in: UdtValue) =>
      in.getList[ev.CassType](fieldName, ev.cassType)
        .asScala
        .map(ev.fromCassandra)
        .toList

  implicit def listUdtValueReads[A](implicit ev: UdtValueReads.Object[A]): UdtValueReads[List[A]] =
    (fieldName: String, in: UdtValue) =>
      in.getList[UdtValue](fieldName, classOf[UdtValue])
        .asScala
        .map(ev.read("unused", _))
        .toList

  implicit def mapBothKeyValPrimValueReads[A, B](implicit
    evA: CassPrimitiveType[A],
    evB: CassPrimitiveType[B]
  ): UdtValueReads[Map[A, B]] =
    (fieldName: String, in: UdtValue) =>
      in.getMap[evA.CassType, evB.CassType](fieldName, evA.cassType, evB.cassType)
        .asScala
        .map { case (casK, casV) => (evA.fromCassandra(casK), evB.fromCassandra(casV)) }
        .toMap

  implicit def mapBothKeyValUdtValueReads[A, B](implicit
    evA: UdtValueReads.Object[A],
    evB: UdtValueReads.Object[B]
  ): UdtValueReads[Map[A, B]] =
    (fieldName: String, in: UdtValue) =>
      in.getMap[UdtValue, UdtValue](fieldName, classOf[UdtValue], classOf[UdtValue])
        .asScala
        .map { case (casK, casV) => (evA.read("unused", casK), evB.read("unused", casV)) }
        .toMap

  implicit def mapKeyPrimValUdtValueReads[A, B](implicit
    evA: CassPrimitiveType[A],
    evB: UdtValueReads.Object[B]
  ): UdtValueReads[Map[A, B]] =
    (fieldName: String, in: UdtValue) =>
      in.getMap[evA.CassType, UdtValue](fieldName, evA.cassType, classOf[UdtValue])
        .asScala
        .map { case (casK, casV) => (evA.fromCassandra(casK), evB.read("unused", casV)) }
        .toMap

  implicit def mapKeyUdtValPrimValueReads[A, B](implicit
    evA: UdtValueReads.Object[A],
    evB: CassPrimitiveType[B]
  ): UdtValueReads[Map[A, B]] =
    (fieldName: String, in: UdtValue) =>
      in.getMap[UdtValue, evB.CassType](fieldName, classOf[UdtValue], evB.cassType)
        .asScala
        .map { case (casK, casV) => (evA.read("unused", casK), evB.fromCassandra(casV)) }
        .toMap
}

trait LowestPriorityUdtValueReads {
  import shapeless._
  import shapeless.labelled._

  implicit def hlistUdtValueReads[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    hUdtValueReads: Lazy[UdtValueReads[H]],
    tUdtValueReads: UdtValueReads[T]
  ): UdtValueReads[FieldType[K, H] :: T] = (_: String, constructor: UdtValue) => {
    // we don't use the fieldName from the function argument for HList but we derive it from the datatype itself
    // we do use the fieldName in the individual instances
    val fieldName  = witness.value.name
    val headReader = hUdtValueReads.value
    val head =
      if (headReader.isObject) nestedCaseClass(fieldName, headReader, constructor)
      else hUdtValueReads.value.read(fieldName, constructor)

    val fieldTypeKH: FieldType[K, H] = field[witness.T](head)
    val tail: T                      = tUdtValueReads.read("unused", constructor)

    fieldTypeKH :: tail
  }

  def nestedCaseClass[A](fieldName: String, reader: UdtValueReads[A], top: UdtValue): A = {
    val nestedUdtValue = top.getUdtValue(fieldName)
    reader.read("unused", nestedUdtValue)
  }

  implicit val hnilUdtValueReads: UdtValueReads[HNil] =
    (_: String, _: UdtValue) => HNil

  implicit def genericUdtValueReads[A, R](implicit
    gen: LabelledGeneric.Aux[A, R],
    enc: Lazy[UdtValueReads[R]],
    evidenceANotOption: A <:!< Option[_]
  ): UdtValueReads.Object[A] =
    (fieldName: String, udtValue: UdtValue) => gen.from(enc.value.read(fieldName, udtValue))
}
