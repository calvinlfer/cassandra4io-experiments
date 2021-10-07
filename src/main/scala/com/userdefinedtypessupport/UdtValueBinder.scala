package com.userdefinedtypessupport

import com.datastax.oss.driver.api.core.`type`._
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{DefaultListType, DefaultMapType, DefaultSetType}
import com.ringcentral.cassandra4io.cql.Binder
import shapeless.Lazy

import java.nio.ByteBuffer
import java.util
import java.util.UUID
import scala.jdk.CollectionConverters._

trait UdtValueBinder[A] { self =>
  def isObject: Boolean = false

  def bind(input: A, fieldName: String, constructor: UdtValue): UdtValue

  def contramap[B](f: B => A): UdtValueBinder[B] = (input: B, fieldName: String, constructor: UdtValue) =>
    self.bind(f(input), fieldName, constructor)
}
object UdtValueBinder
    extends MidPriorityUdtValueBinder
    with LowerPriorityUdtValueBinder
    with LowestPriorityUdtValueBinder {
  // This is used to determine whether we have a UdtValueBinder for a case class which corresponds to a UdtValue in Cassandra
  trait Object[A] extends UdtValueBinder[A] {
    override def isObject: Boolean = true
  }

  def deriveTopLevel[A](implicit udtValueBinder: UdtValueBinder.Object[A]): Binder[A] = {
    (statement: BoundStatement, index: Int, value: A) =>
      val userDefinedType =
        statement.getPreparedStatement.getVariableDefinitions
          .get(index)
          .getType
          .asInstanceOf[UserDefinedType]
      (
        statement.setUdtValue(
          index,
          udtValueBinder.bind(value, "unused", userDefinedType.newValue())
        ),
        index + 1
      )
  }

  // Summon typeclass instances that can only be built by Shapeless machinery for HLists because ultimately this is
  // what a UDT is (from a case class)
  def apply[A](implicit top: UdtValueBinder.Object[A]): UdtValueBinder.Object[A] = top

  implicit val stringUdtValueBinder: UdtValueBinder[String] =
    (in: String, fieldName: String, constructor: UdtValue) => constructor.setString(fieldName, in)

  implicit val intUdtValueBinder: UdtValueBinder[Int] =
    (in: Int, fieldName: String, constructor: UdtValue) => constructor.setInt(fieldName, in)

  implicit val longUdtValueBinder: UdtValueBinder[Long] =
    (in: Long, fieldName: String, constructor: UdtValue) => constructor.setLong(fieldName, in)

  implicit val doubleUdtValueBinder: UdtValueBinder[Double] =
    (in: Double, fieldName: String, constructor: UdtValue) => constructor.setDouble(fieldName, in)

  implicit val bigDecimalUdtValueBinder: UdtValueBinder[BigDecimal] =
    (in: BigDecimal, fieldName: String, constructor: UdtValue) => constructor.setBigDecimal(fieldName, in.bigDecimal)

  implicit val bigIntUdtValueBinder: UdtValueBinder[BigInt] =
    (in: BigInt, fieldName: String, constructor: UdtValue) => constructor.setBigInteger(fieldName, in.bigInteger)

  implicit val booleanUdtValueBinder: UdtValueBinder[Boolean] =
    (in: Boolean, fieldName: String, constructor: UdtValue) => constructor.setBoolean(fieldName, in)

  implicit val uuidUdtValueBinder: UdtValueBinder[UUID] =
    (in: UUID, fieldName: String, constructor: UdtValue) => constructor.setUuid(fieldName, in)

  implicit val byteBufferUdtValueBinder: UdtValueBinder[ByteBuffer] =
    (in: ByteBuffer, fieldName: String, constructor: UdtValue) => constructor.setByteBuffer(fieldName, in)
}

trait MidPriorityUdtValueBinder {
  implicit def nestedOneLevelSetUdtValueBinder[A](implicit
    ev: UdtValueBinder.Object[A]
  ): UdtValueBinder[Set[Set[A]]] = { (setSetA, fieldName, topUdtValue) =>
    val innermostElemConstructor =
      topUdtValue
        .getType(fieldName)
        .asInstanceOf[DefaultSetType] // Outer Set
        .getElementType
        .asInstanceOf[DefaultSetType] // Inner Set
        .getElementType
        .asInstanceOf[UserDefinedType] // UDT
    val serialized: util.Set[util.Set[UdtValue]] =
      setSetA.map(setA => setA.map(a => ev.bind(a, "unused", innermostElemConstructor.newValue())).asJava).asJava

    val typeToken = new GenericType[util.Set[util.Set[UdtValue]]] {}
    topUdtValue.set(fieldName, serialized, typeToken)
  }

  implicit def nestedOneLevelSetPrimValueBinder[A](implicit
    ev: CassPrimitiveType[A]
  ): UdtValueBinder[Set[Set[A]]] = { (setSetA, fieldName, topUdtValue) =>
    val serialized: util.Set[util.Set[ev.CassType]] =
      setSetA.map(setA => setA.map(a => ev.toCassandra(a)).asJava).asJava

    val typeToken = new GenericType[util.Set[util.Set[ev.CassType]]] {}
    topUdtValue.set(fieldName, serialized, typeToken)
  }
}

trait LowerPriorityUdtValueBinder {
  // For Option[CaseClass/UdtValue]
  implicit def optionUdtValueBinder[A](implicit udtValueBinderA: UdtValueBinder.Object[A]): UdtValueBinder[Option[A]] =
    (in: Option[A], fieldName: String, constructor: UdtValue) =>
      in.fold(constructor)(a => udtValueBinderA.bind(a, fieldName, constructor))

  // For Option[Cassandra Primitive - Int/String/Long]
  implicit def optionPrimValueBinder[A](implicit prim: CassPrimitiveType[A]): UdtValueBinder[Option[A]] =
    (in: Option[A], fieldName: String, constructor: UdtValue) =>
      in.fold(constructor)(a => constructor.set[prim.CassType](fieldName, prim.toCassandra(a), prim.cassType))

  implicit def setUdtValueBinder[A](implicit ev: UdtValueBinder.Object[A]): UdtValueBinder[Set[A]] = {
    (setA, fieldName, topUdtValue) =>
      val datatype =
        topUdtValue.getType(fieldName).asInstanceOf[DefaultSetType].getElementType.asInstanceOf[UserDefinedType]
      topUdtValue
        .setSet[UdtValue](fieldName, setA.map(ev.bind(_, "unused", datatype.newValue())).asJava, classOf[UdtValue])
  }

  implicit def setPrimValueBinder[A](implicit ev: CassPrimitiveType[A]): UdtValueBinder[Set[A]] = {
    (setA, fieldName, topUdtValue) =>
      topUdtValue.setSet[ev.CassType](fieldName, setA.map(ev.toCassandra(_)).asJava, ev.cassType)
  }

  implicit def listUdtValueBinder[A](implicit ev: UdtValueBinder.Object[A]): UdtValueBinder[List[A]] = {
    (listA, fieldName, topUdtValue) =>
      val dataType =
        topUdtValue.getType(fieldName).asInstanceOf[DefaultListType].getElementType.asInstanceOf[UserDefinedType]
      val listUdt = listA.map(ev.bind(_, "unused", dataType.newValue())).asJava
      topUdtValue.setList[UdtValue](fieldName, listUdt, classOf[UdtValue])
  }

  implicit def listPrimValueBinder[A](implicit ev: CassPrimitiveType[A]): UdtValueBinder[List[A]] = {
    (setA, fieldName, topUdtValue) =>
      val serialized = setA.map(a => ev.toCassandra(a)).asJava
      topUdtValue.setList[ev.CassType](fieldName, serialized, ev.cassType)
  }

  implicit def mapKeyUdtValueUdtBinder[A, B](implicit
    evA: UdtValueBinder.Object[A],
    evB: UdtValueBinder.Object[B]
  ): UdtValueBinder[Map[A, B]] = { (input: Map[A, B], fieldName: String, topUdtValue: UdtValue) =>
    val dataTypeKey =
      topUdtValue.getType(fieldName).asInstanceOf[DefaultMapType].getKeyType.asInstanceOf[UserDefinedType]
    val dataTypeValue =
      topUdtValue.getType(fieldName).asInstanceOf[DefaultMapType].getKeyType.asInstanceOf[UserDefinedType]
    val serialized =
      input.map { case (k, v) =>
        (evA.bind(k, "unused", dataTypeKey.newValue()), evB.bind(v, "unused", dataTypeValue.newValue()))
      }.asJava

    topUdtValue.setMap[UdtValue, UdtValue](fieldName, serialized, classOf[UdtValue], classOf[UdtValue])
  }

  implicit def mapKeyUdtValuePrimBinder[A, B](implicit
    udtA: UdtValueBinder.Object[A],
    primB: CassPrimitiveType[B]
  ): UdtValueBinder[Map[A, B]] = { (input: Map[A, B], fieldName: String, topUdtValue: UdtValue) =>
    val dataTypeKey =
      topUdtValue.getType(fieldName).asInstanceOf[DefaultMapType].getKeyType.asInstanceOf[UserDefinedType]
    val serialized = input.map { case (k, v) =>
      (udtA.bind(k, "unused", dataTypeKey.newValue()), primB.toCassandra(v))
    }.asJava
    topUdtValue.setMap[UdtValue, primB.CassType](fieldName, serialized, classOf[UdtValue], primB.cassType)
  }

  implicit def mapKeyPrimValueUdtBinder[A, B](implicit
    primA: CassPrimitiveType[A],
    udtB: UdtValueBinder.Object[B]
  ): UdtValueBinder[Map[A, B]] = { (input: Map[A, B], fieldName: String, topUdtValue: UdtValue) =>
    val dataTypeValue =
      topUdtValue.getType(fieldName).asInstanceOf[DefaultMapType].getValueType.asInstanceOf[UserDefinedType]
    val serialized = input.map { case (k, v) =>
      (primA.toCassandra(k), udtB.bind(v, "unused", dataTypeValue.newValue()))
    }.asJava
    topUdtValue.setMap[primA.CassType, UdtValue](fieldName, serialized, primA.cassType, classOf[UdtValue])
  }

  implicit def mapKeyPrimValuePrimBinder[A, B](implicit
    primA: CassPrimitiveType[A],
    primB: CassPrimitiveType[B]
  ): UdtValueBinder[Map[A, B]] = { (input: Map[A, B], fieldName: String, topUdtValue: UdtValue) =>
    val serialized = input.map { case (k, v) => (primA.toCassandra(k), primB.toCassandra(v)) }.asJava
    topUdtValue.setMap[primA.CassType, primB.CassType](fieldName, serialized, primA.cassType, primB.cassType)
  }
}

trait LowestPriorityUdtValueBinder {
  import shapeless._
  import shapeless.labelled._

  implicit def hlistUdtValueBinder[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    hUdtValueBinder: Lazy[UdtValueBinder[H]],
    tUdtValueBinder: UdtValueBinder[T]
  ): UdtValueBinder[FieldType[K, H] :: T] = (in: FieldType[K, H] :: T, _: String, constructor: UdtValue) => {
    // we don't use the fieldName from the function argument for HList but we derive it from the datatype itself
    // we do use the fieldName in the individual instances
    val headValue  = in.head
    val fieldName  = witness.value.name
    val headBinder = hUdtValueBinder.value

    val nextConstructor =
      if (headBinder.isObject) handleNestedCaseClass(fieldName, headValue, headBinder, constructor)
      else hUdtValueBinder.value.bind(headValue, fieldName, constructor)

    tUdtValueBinder.bind(in.tail, fieldName, nextConstructor)
  }

  def handleNestedCaseClass[A](fieldName: String, in: A, ev: UdtValueBinder[A], top: UdtValue): UdtValue = {
    val constructor = top.getType(fieldName).asInstanceOf[UserDefinedType].newValue()
    val serialized  = ev.bind(in, "unused", constructor)
    top.setUdtValue(fieldName, serialized)
  }

  implicit val hnilUdtValueBinder: UdtValueBinder[HNil] =
    (_: HNil, _: String, constructor: UdtValue) => constructor

  implicit def genericUdtValueBinder[A, R](implicit
    gen: LabelledGeneric.Aux[A, R],
    enc: Lazy[UdtValueBinder[R]],
    evidenceANotOption: A <:!< Option[_]
  ): UdtValueBinder.Object[A] = (in: A, fieldName: String, constructor: UdtValue) =>
    enc.value.bind(gen.to(in), fieldName, constructor)
}

object BinderExtras {
  // top level Set containing a UDT
  implicit def setBinderA[A](implicit udtBinder: Lazy[UdtValueBinder.Object[A]]): Binder[Set[A]] = {
    (statement: BoundStatement, index: Int, value: Set[A]) =>
      val userDefinedType =
        statement.getPreparedStatement.getVariableDefinitions
          .get(index)
          .getType
          .asInstanceOf[DefaultSetType]
          .getElementType
          .asInstanceOf[UserDefinedType]
      val setUdt = value.map(udtBinder.value.bind(_, "unused", userDefinedType.newValue())).asJava
      (statement.setSet[UdtValue](index, setUdt, classOf[UdtValue]), index + 1)
  }
}
