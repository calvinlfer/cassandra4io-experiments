package com.experiments.calvin

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.data.UdtValue
import com.ringcentral.cassandra4io.cql.Binder

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

trait UdtValueBinder[A] { self =>
  def bind(input: A, fieldName: String, constructor: UdtValue): UdtValue

  def contramap[B](f: B => A): UdtValueBinder[B] = new UdtValueBinder[B] {
    override def bind(input: B, fieldName: String, constructor: UdtValue): UdtValue =
      self.bind(f(input), fieldName, constructor)
  }
}

object UdtValueBinder extends UdtValueBinderLowPriority {
  def deriveBinder[A](implicit udtValueBinder: UdtValueBinder[A]): Binder[A] = {
    (statement: BoundStatement, index: Int, value: A) =>
      val userDefinedType =
        statement.getPreparedStatement.getVariableDefinitions.get(index).getType.asInstanceOf[UserDefinedType]
      val write = udtValueBinder.bind(value, "not-used", userDefinedType.newValue())
      (statement.setUdtValue(index, write), index + 1)
  }

  def apply[A](implicit udtValueBinder: UdtValueBinder[A]): UdtValueBinder[A] = udtValueBinder

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

  implicit def optionUdtValueBinder[A](implicit udtValueBinderA: UdtValueBinder[A]): UdtValueBinder[Option[A]] =
    (in: Option[A], fieldName: String, constructor: UdtValue) =>
      in.fold(constructor)(a => udtValueBinderA.bind(a, fieldName, constructor))

  implicit def seqUdtValueBinder[A](implicit
    udtValueBinderA: UdtValueBinder[A],
    classTagA: ClassTag[A]
  ): UdtValueBinder[Seq[A]] =
    (in: Seq[A], fieldName: String, constructor: UdtValue) =>
      constructor.setList(fieldName, in.asJava, classTagA.runtimeClass.asInstanceOf[java.lang.Class[A]])

  implicit def setUdtValueBinder[A](implicit
    udtValueBinderA: UdtValueBinder[A],
    classTagA: ClassTag[A]
  ): UdtValueBinder[Set[A]] =
    (in: Set[A], fieldName: String, constructor: UdtValue) =>
      constructor.setSet(fieldName, in.asJava, classTagA.runtimeClass.asInstanceOf[java.lang.Class[A]])

  implicit def mapUdtValueBinder[K, V](implicit
    udtValueBinderK: UdtValueBinder[K],
    udtValueBinderV: UdtValueBinder[V],
    classTagK: ClassTag[K],
    classTagV: ClassTag[V]
  ): UdtValueBinder[Map[K, V]] =
    (in: Map[K, V], fieldName: String, constructor: UdtValue) =>
      constructor.setMap(
        fieldName,
        in.asJava,
        classTagK.runtimeClass.asInstanceOf[java.lang.Class[K]],
        classTagV.runtimeClass.asInstanceOf[java.lang.Class[V]]
      )
}

trait UdtValueBinderLowPriority {
  import shapeless._
  import shapeless.labelled._

  implicit def hlistUdtValueBinder[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    hUdtValueReads: Lazy[UdtValueBinder[H]],
    tUdtValueReads: UdtValueBinder[T]
  ): UdtValueBinder[FieldType[K, H] :: T] = (in: FieldType[K, H] :: T, _: String, constructor: UdtValue) => {
    // we don't use the fieldName from the function argument for HList but we derive it from the datatype itself
    // we do use the fieldName in the individual instances
    val headValue       = in.head
    val fieldName       = witness.value.name
    val nextConstructor = hUdtValueReads.value.bind(headValue, fieldName, constructor)
    tUdtValueReads.bind(in.tail, fieldName, nextConstructor)
  }

  implicit val hnilUdtValueBinder: UdtValueBinder[HNil] =
    (_: HNil, _: String, constructor: UdtValue) => constructor

  implicit def genericUdtValueBinder[A, R](implicit
    gen: LabelledGeneric.Aux[A, R],
    enc: Lazy[UdtValueBinder[R]],
    evidenceANotOption: A <:!< Option[_]
  ): UdtValueBinder[A] = (in: A, fieldName: String, constructor: UdtValue) =>
    enc.value.bind(gen.to(in), fieldName, constructor)
}
