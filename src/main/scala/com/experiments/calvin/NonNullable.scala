package com.experiments.calvin
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Row}
import com.ringcentral.cassandra4io.cql.{Binder, Reads}
import shapeless.<:!<
import zio.prelude._

final case class NonNullableConstraintViolated(message: String) extends Exception

final case class NonNullable[A](underlying: A) extends Subtype[A]
object NonNullable {
  implicit def nonNullableCqlReads[A](implicit
    underlyingReads: Reads[A],
    evidenceNotNested: A <:!< NonNullable[_]
  ): Reads[NonNullable[A]] = { (row: Row, index: Int) =>
    if (row.isNull(index))
      throw NonNullableConstraintViolated(
        s"Expected valid data at index $index at row ${row.getFormattedContents} but got NULL instead"
      )
    else {
      val (underlying, nextIndex) = underlyingReads.read(row, index)
      (NonNullable(underlying), nextIndex)
    }
  }

  implicit def nonNullableCqlBinder[A](implicit
    underlyingBinder: Binder[A],
    evidenceNotOption: A <:!< Option[_],
    evidenceNotNested: A <:!< NonNullable[_]
  ): Binder[NonNullable[A]] =
    (statement: BoundStatement, index: Int, value: NonNullable[A]) =>
      underlyingBinder.bind(statement, index, value.underlying)
}
