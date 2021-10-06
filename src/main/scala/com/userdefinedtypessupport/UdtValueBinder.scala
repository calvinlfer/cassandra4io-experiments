package com.userdefinedtypessupport

import com.datastax.oss.driver.api.core.`type`._
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{DefaultListType, DefaultMapType, DefaultSetType}
import com.userdefinedtypessupport.CassPrimitiveType.{ByteBuf, NotSupported}
import com.ringcentral.cassandra4io.cql.Binder
import shapeless.Lazy

import java.nio.ByteBuffer
import java.util
import java.util.UUID
import scala.jdk.CollectionConverters._

// A compile-time safe alternative to reflection for primitive types residing within Cassandra User Defined Types
// This typeclass is used to hold onto the associated Cassandra types associated with the Scala types for the underlying Datastax API
sealed trait CassPrimitiveType[A]
object CassPrimitiveType extends CassPrimitiveTypeLowPriority {
  sealed trait Supported[A] extends CassPrimitiveType[A] {
    type Output
    def cassType: Class[Output]
    def output(in: A): Output
  }
  implicit case object Short extends Supported[Short] {
    type Output = java.lang.Short
    def cassType: Class[Output]   = classOf[java.lang.Short]
    def output(in: Short): Output = scala.Short.box(in)
  }
  implicit case object Int extends Supported[Int] {
    type Output = java.lang.Integer
    def cassType: Class[Output] = classOf[java.lang.Integer]
    def output(in: Int): Output = scala.Int.box(in)
  }
  implicit case object Dbl extends Supported[Double] {
    type Output = Double
    def cassType: Class[Output]    = classOf[Double]
    def output(in: Double): Output = in
  }
  implicit case object Lng extends Supported[Long] {
    type Output = Long
    def cassType: Class[Output]  = classOf[Long]
    def output(in: Long): Output = in
  }
  implicit case object Str extends Supported[String] {
    type Output = String
    def cassType: Class[Output]    = classOf[String]
    def output(in: String): Output = in
  }
  implicit case object ByteBuf extends Supported[ByteBuffer] {
    override type Output = java.nio.ByteBuffer

    override def cassType: Class[Output] = classOf[java.nio.ByteBuffer]

    override def output(in: ByteBuffer): Output = in
  }

  // If you don't have an instance for one of the primitive types then you automatically get one of these
  // We use this to simulate conditional implicits by only asking for CassPrimitiveType.Supported
  trait NotSupported[A] extends CassPrimitiveType[A]
}
trait CassPrimitiveTypeLowPriority {
  implicit def notSupported[A]: CassPrimitiveType.NotSupported[A] = new NotSupported[A] {
    override def toString = "NotSupported"
  }
}

trait UdtValueBinder[A] { self =>
  def bind(input: A, fieldName: String, constructor: UdtValue): UdtValue

  def contramap[B](f: B => A): UdtValueBinder[B] = (input: B, fieldName: String, constructor: UdtValue) =>
    self.bind(f(input), fieldName, constructor)
}
object UdtValueBinder extends LowerPriorityUdtValueBinder with LowestPriorityUdtValueBinder {
  // This is used to determine whether we have a UdtValueBinder for a case class which corresponds to a UdtValue in Cassandra
  trait Object[A] extends UdtValueBinder[A]

  def apply[A](implicit ev: UdtValueBinder[A]): UdtValueBinder[A] = ev

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
}

trait LowerPriorityUdtValueBinder {
  // For Option[CaseClass/UdtValue]
  implicit def optionUdtValueBinder[A](implicit udtValueBinderA: UdtValueBinder.Object[A]): UdtValueBinder[Option[A]] =
    (in: Option[A], fieldName: String, constructor: UdtValue) =>
      in.fold(constructor)(a => udtValueBinderA.bind(a, fieldName, constructor))

  // For Option[Cassandra Primitive - Int/String/Long]
  implicit def optionPrimValueBinder[A](implicit prim: CassPrimitiveType.Supported[A]): UdtValueBinder[Option[A]] =
    (in: Option[A], fieldName: String, constructor: UdtValue) =>
      in.fold(constructor)(a => constructor.set[prim.Output](fieldName, prim.output(a), prim.cassType))

  implicit def setUdtValueBinder[A](implicit ev: UdtValueBinder.Object[A]): UdtValueBinder[Set[A]] = {
    (setA, fieldName, topUdtValue) =>
      val datatype =
        topUdtValue.getType(fieldName).asInstanceOf[DefaultSetType].getElementType.asInstanceOf[UserDefinedType]
      topUdtValue
        .setSet[UdtValue](fieldName, setA.map(ev.bind(_, "unused", datatype.newValue())).asJava, classOf[UdtValue])
  }

  implicit def setPrimValueBinder[A](implicit ev: CassPrimitiveType.Supported[A]): UdtValueBinder[Set[A]] = {
    (setA, fieldName, topUdtValue) =>
      topUdtValue.setSet[ev.Output](fieldName, setA.map(ev.output(_)).asJava, ev.cassType)
  }

  implicit def listUdtValueBinder[A](implicit ev: UdtValueBinder.Object[A]): UdtValueBinder[List[A]] = {
    (listA, fieldName, topUdtValue) =>
      val dataType =
        topUdtValue.getType(fieldName).asInstanceOf[DefaultListType].getElementType.asInstanceOf[UserDefinedType]
      val listUdt = listA.map(ev.bind(_, "unused", dataType.newValue())).asJava
      topUdtValue.setList[UdtValue](fieldName, listUdt, classOf[UdtValue])
  }

  implicit def listPrimValueBinder[A](implicit ev: CassPrimitiveType.Supported[A]): UdtValueBinder[List[A]] = {
    (setA, fieldName, topUdtValue) =>
      val serialized = setA.map(a => ev.output(a)).asJava
      topUdtValue.setList[ev.Output](fieldName, serialized, ev.cassType)
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
    primB: CassPrimitiveType.Supported[B]
  ): UdtValueBinder[Map[A, B]] = { (input: Map[A, B], fieldName: String, topUdtValue: UdtValue) =>
    val dataTypeKey =
      topUdtValue.getType(fieldName).asInstanceOf[DefaultMapType].getKeyType.asInstanceOf[UserDefinedType]
    val serialized = input.map { case (k, v) =>
      (udtA.bind(k, "unused", dataTypeKey.newValue()), primB.output(v))
    }.asJava
    topUdtValue.setMap[UdtValue, primB.Output](fieldName, serialized, classOf[UdtValue], primB.cassType)
  }

  implicit def mapKeyPrimValueUdtBinder[A, B](implicit
    primA: CassPrimitiveType.Supported[A],
    udtB: UdtValueBinder.Object[B]
  ): UdtValueBinder[Map[A, B]] = { (input: Map[A, B], fieldName: String, topUdtValue: UdtValue) =>
    val dataTypeValue =
      topUdtValue.getType(fieldName).asInstanceOf[DefaultMapType].getValueType.asInstanceOf[UserDefinedType]
    val serialized = input.map { case (k, v) =>
      (primA.output(k), udtB.bind(v, "unused", dataTypeValue.newValue()))
    }.asJava
    topUdtValue.setMap[primA.Output, UdtValue](fieldName, serialized, primA.cassType, classOf[UdtValue])
  }

  implicit def mapKeyPrimValuePrimBinder[A, B](implicit
    primA: CassPrimitiveType.Supported[A],
    primB: CassPrimitiveType.Supported[B]
  ): UdtValueBinder[Map[A, B]] = { (input: Map[A, B], fieldName: String, topUdtValue: UdtValue) =>
    val serialized = input.map { case (k, v) => (primA.output(k), primB.output(v)) }.asJava
    topUdtValue.setMap[primA.Output, primB.Output](fieldName, serialized, primA.cassType, primB.cassType)
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
    val headValue = in.head
    val fieldName = witness.value.name

    val nextConstructor = hUdtValueBinder.value.bind(headValue, fieldName, constructor)
    tUdtValueBinder.bind(in.tail, fieldName, nextConstructor)
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

  def binderUdt[A](implicit udtValueBinder: Lazy[UdtValueBinder.Object[A]]): Binder[A] = {
    (statement: BoundStatement, index: Int, value: A) =>
      val userDefinedType =
        statement.getPreparedStatement.getVariableDefinitions
          .get(index)
          .getType
          .asInstanceOf[UserDefinedType]
      (
        statement.setUdtValue(
          index,
          udtValueBinder.value.bind(value, "unused", userDefinedType.newValue())
        ),
        index + 1
      )
  }
}
