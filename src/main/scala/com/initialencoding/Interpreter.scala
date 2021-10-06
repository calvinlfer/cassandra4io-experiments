package com.initialencoding

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.DefaultListType
import com.initialencoding.CassandraData.{Collection, Primitive}

import scala.jdk.CollectionConverters._

object Interpreter {
  def interpretUDT(data: CassandraData.CUDT, top: UserDefinedType): UdtValue = {
    val udtValue = top.newValue()
    data.pairs.foldLeft(udtValue) { case (acc, LabelledData(key, value)) =>
      value match {
        case primitive: CassandraData.Primitive =>
          insertPrimitive(acc, key, primitive)

        case collection: CassandraData.Collection =>
          collection match {
            case Collection.CList(values) =>
              insertList(acc, key, values)

            case Collection.CSet(values) =>
              ???

            case Collection.CMap(pairs) =>
              ???
          }

        case udt @ CassandraData.CUDT(_) =>
          val top = acc.getType(key).asInstanceOf[UserDefinedType]
          interpretUDT(udt, top)
      }
    }
  }

  def insertPrimitive(udt: UdtValue, key: String, value: CassandraData.Primitive): UdtValue =
    value match {
      case Primitive.CStr(value) => udt.setString(key, value)
      case Primitive.CInt(value) => udt.setInt(key, value)
      case Primitive.CDbl(value) => udt.setDouble(key, value)
      case Primitive.CLng(value) => udt.setLong(key, value)
    }

  def insertList(udt: UdtValue, key: String, values: List[CassandraData]): UdtValue =
    values.headOption match {
      case Some(value) =>
        value match {
          case primitive: Primitive =>
            primitive match {
              case Primitive.CStr(_) =>
                udt.setList[String](key, values.collect { case Primitive.CStr(value) => value }.asJava, classOf[String])

              case Primitive.CInt(_) =>
                udt.setList[java.lang.Integer](
                  key,
                  values.collect { case Primitive.CInt(value) => Int.box(value) }.asJava,
                  classOf[java.lang.Integer]
                )

              case Primitive.CDbl(_) =>
                udt.setList[Double](
                  key,
                  values.collect { case Primitive.CDbl(value) => value }.asJava,
                  classOf[Double]
                )

              case Primitive.CLng(_) =>
                udt.setList[Long](
                  key,
                  values.collect { case Primitive.CLng(value) => value }.asJava,
                  classOf[Long]
                )
            }

          // handling nested collections is complicated
          case collection: Collection =>
            ???

          case data @ CassandraData.CUDT(pairs) =>
            val userDefinedType =
              udt.getType(key).asInstanceOf[DefaultListType].getElementType.asInstanceOf[UserDefinedType]
            udt.setList[UdtValue](
              key,
              values.collect { case cudt @ CassandraData.CUDT(_) => interpretUDT(cudt, userDefinedType) }.asJava,
              classOf[UdtValue]
            )
        }

      case None =>
        udt
    }
}
