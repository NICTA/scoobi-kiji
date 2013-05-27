package com.nicta.scoobi
package io
package kiji
package testing
package mutable

import scalaz.Scalaz._

/**
 * This specification class is for Scoobi code which is going to use a Kiji database
 */
abstract class KijiSpecification extends com.nicta.scoobi.testing.mutable.ScoobiSpecificationLike {
  override def context = super.context.flatMap(sc => new KijiContext(sc))
}
