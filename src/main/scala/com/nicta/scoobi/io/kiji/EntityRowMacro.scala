package com.nicta.scoobi
package io
package kiji

import scala.reflect.macros.Context
import shapeless._

object EntityRowMacro {
//  def recentValues[H <: HList : KijiFormat](c : Context)(family: c.Expr[String])(qualifiers: c.Expr[Seq[String]]) = {
//    import c.{universe => u}; import u._
////    val values = qualifiers.map(qualifier => data.getMostRecentValue[Any](family, qualifier))
////    val format = implicitly[KijiFormat[H]]
////    format.fromKiji(values)
//    reify(HNil)
//  }
}
