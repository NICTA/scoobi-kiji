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
package mutable

import application.ScoobiAppConfiguration
import org.specs2.specification.Groups
import core.ScoobiConfiguration

/**
 * This Scoobi specification allows to write specs2 "mutable scripts" with Scoobi configuration objects:
 *
 * class MySpec extends ScoobiSpecification with Groups { s2"""
 *
 * List operations
 * ===============
 *
 *  This is an example of using Scoobi DLists
 *   + you can map over a DList
 *   + you can get the size of a DList
 *
 * Input outputs
 * =============
 *
 *  + You can create a DList from a text file
 *  + or from an Avro file
 *
 *                                            """
 *  "list operations" - new group {
 *    eg := DList(1, 2, 3).map(_ + 1).run === Vector(2, 3, 4)
 *    eg := DList(1, 2, 3).size.run === 3
 *  }
 *  "input-outputs" - new group {
 *    eg := fromTextFile("path").isEmpty must beFalse
 *    eg := fromAvroFile("path").isEmpty must beFalse
 *  }
 *
 * }
 */
abstract class ScoobiSpecification extends ScoobiSpecificationLike

/**
 * extracted trait for reusability
 */
trait ScoobiSpecificationLike extends HadoopSpecificationStructure with org.specs2.mutable.script.SpecificationLike with ScoobiAppConfiguration {
  type SC = ScoobiConfiguration
  // this configuration object needs to be explicit (rather than implicit)
  // otherwise it will clash with the implicit sc: ScoobiConfiguration declaration that's used for each example
  // this configuration object is used by the ClusterConfiguration trait to determine the settings for fs/jobTracker
  override lazy val configuration = super[ScoobiAppConfiguration].configuration
}
