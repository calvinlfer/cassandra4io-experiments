package com.userdefinedtypessupport

import java.nio.ByteBuffer

// A compile-time safe alternative to reflection for primitive types residing within collections within Cassandra User Defined Types
// This typeclass is used to hold onto the associated Cassandra types associated with the Scala types for the underlying Datastax API
// and handle boxing where needed
sealed trait CassPrimitiveType[A] {
  type CassType
  def cassType: Class[CassType]
  def toCassandra(in: A): CassType
  def fromCassandra(in: CassType): A
}
object CassPrimitiveType {
  implicit case object Short extends CassPrimitiveType[Short] {
    type CassType = java.lang.Short
    def cassType: Class[CassType]          = classOf[java.lang.Short]
    def toCassandra(in: Short): CassType   = scala.Short.box(in)
    def fromCassandra(in: CassType): Short = in.shortValue()
  }

  implicit case object Int extends CassPrimitiveType[Int] {
    type CassType = java.lang.Integer
    def cassType: Class[CassType]        = classOf[java.lang.Integer]
    def toCassandra(in: Int): CassType   = scala.Int.box(in)
    def fromCassandra(in: CassType): Int = in.intValue()
  }

  implicit case object Dbl extends CassPrimitiveType[Double] {
    type CassType = Double
    def cassType: Class[CassType]           = classOf[Double]
    def toCassandra(in: Double): CassType   = in
    def fromCassandra(in: CassType): Double = in
  }

  implicit case object Lng extends CassPrimitiveType[Long] {
    type CassType = Long
    def cassType: Class[CassType]         = classOf[Long]
    def toCassandra(in: Long): CassType   = in
    def fromCassandra(in: CassType): Long = in
  }

  implicit case object Str extends CassPrimitiveType[String] {
    type CassType = String
    def cassType: Class[CassType]           = classOf[String]
    def toCassandra(in: String): CassType   = in
    def fromCassandra(in: CassType): String = in
  }

  implicit case object ByteBuf extends CassPrimitiveType[ByteBuffer] {
    type CassType = java.nio.ByteBuffer
    def cassType: Class[CassType]               = classOf[java.nio.ByteBuffer]
    def toCassandra(in: ByteBuffer): CassType   = in
    def fromCassandra(in: CassType): ByteBuffer = in
  }

  implicit case object BigDecimal extends CassPrimitiveType[scala.BigDecimal] {
    type CassType = java.math.BigDecimal
    def cassType: Class[CassType]                     = classOf[java.math.BigDecimal]
    def toCassandra(in: scala.BigDecimal): CassType   = in.bigDecimal
    def fromCassandra(in: CassType): scala.BigDecimal = in
  }

  implicit case object BigInt extends CassPrimitiveType[scala.BigInt] {
    type CassType = java.math.BigInteger
    def cassType: Class[CassType]                 = classOf[java.math.BigInteger]
    def toCassandra(in: scala.BigInt): CassType   = in.bigInteger
    def fromCassandra(in: CassType): scala.BigInt = in
  }
}

/**
 * Note rather than writing this twice in the form of separate instance(s) for UdtValueBinder[String/Int/etc.], we can do this
 * but it comes at the cost of performance
 *  implicit def primitiveBinder[A](implicit ev: CassPrimitiveType[A]): UdtValueBinder[A] =
 *    (in: A, fieldName: String, constructor: UdtValue) =>
 *       constructor.set[ev.CassType](fieldName, ev.toCassandra(in), ev.cassType)
 */
