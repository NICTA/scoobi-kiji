package com.nicta.scoobi
package io
package kiji
package testing
package mutable

import scalaz.Scalaz._

abstract class KijiSpecification extends com.nicta.scoobi.testing.mutable.ScoobiSpecificationLike {
  override def context = super.context.flatMap(sc => new KijiContext(sc))
}
