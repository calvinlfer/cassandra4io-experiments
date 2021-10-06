package com.userdefinedtypessupport

object TestPrim extends App {
  final case class Example()
  println(implicitly[CassPrimitiveType[Example]].toString)
}
