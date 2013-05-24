package com.nicta.scoobi
package testing
package mutable

import application.ScoobiAppConfiguration
import org.specs2.specification.Groups
import core.ScoobiConfiguration

abstract class ScoobiSpecification extends ScoobiSpecificationLike
trait ScoobiSpecificationLike extends HadoopSpecificationStructure with org.specs2.mutable.script.SpecificationLike with Groups  with ScoobiAppConfiguration {
  type SC = ScoobiConfiguration
  // this configuration object needs to be explicit (rather than implicit)
  // otherwise it will clash with the implicit sc: ScoobiConfiguration declaration that's used for each example
  // this configuration object is used by the ClusterConfiguration trait to determine the settings for fs/jobTracker
  override lazy val configuration = super[ScoobiAppConfiguration].configuration
}
