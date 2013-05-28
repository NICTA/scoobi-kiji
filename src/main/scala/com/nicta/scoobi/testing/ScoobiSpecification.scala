/**
 * Copyright 2011,2012 National ICT Australia Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nicta.scoobi
package testing

import org.specs2.specification._
import application.ScoobiAppConfiguration
import core.ScoobiConfiguration

abstract class ScoobiSpecification extends ScoobiSpecificationLike
trait ScoobiSpecificationLike extends HadoopSpecificationStructure with script.SpecificationLike with ScoobiAppConfiguration {
 type SC = ScoobiConfiguration
  // this configuration object needs to be explicit (rather than implicit)
  // otherwise it will clash with the implicit sc: ScoobiConfiguration declaration that's used for each example
  // this configuration object is used by the ClusterConfiguration trait to determine the settings for fs/jobTracker
  override lazy val configuration = super[ScoobiAppConfiguration].configuration
}
