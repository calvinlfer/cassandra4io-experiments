package com.userdefinedtypessupport

import java.nio.ByteBuffer

// A compile-time safe alternative to reflection for primitive types residing within Cassandra User Defined Types
// This typeclass is used to hold onto the associated Cassandra types associated with the Scala types for the underlying Datastax API
// and handle boxing where needed
sealed trait CassPrimitiveTypeBinder[A] {
  type Output
  def cassType: Class[Output]
  def output(in: A): Output
}
object CassPrimitiveTypeBinder {
  implicit case object Short extends CassPrimitiveTypeBinder[Short] {
    type Output = java.lang.Short
    def cassType: Class[Output]   = classOf[java.lang.Short]
    def output(in: Short): Output = scala.Short.box(in)
  }
  implicit case object Int extends CassPrimitiveTypeBinder[Int] {
    type Output = java.lang.Integer
    def cassType: Class[Output] = classOf[java.lang.Integer]
    def output(in: Int): Output = scala.Int.box(in)
  }
  implicit case object Dbl extends CassPrimitiveTypeBinder[Double] {
    type Output = Double
    def cassType: Class[Output]    = classOf[Double]
    def output(in: Double): Output = in
  }
  implicit case object Lng extends CassPrimitiveTypeBinder[Long] {
    type Output = Long
    def cassType: Class[Output]  = classOf[Long]
    def output(in: Long): Output = in
  }
  implicit case object Str extends CassPrimitiveTypeBinder[String] {
    type Output = String
    def cassType: Class[Output]    = classOf[String]
    def output(in: String): Output = in
  }
  implicit case object ByteBuf extends CassPrimitiveTypeBinder[ByteBuffer] {
    override type Output = java.nio.ByteBuffer
    override def cassType: Class[Output]        = classOf[java.nio.ByteBuffer]
    override def output(in: ByteBuffer): Output = in
  }
}
