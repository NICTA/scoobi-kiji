/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nicta.scoobi
package io
package kiji

import org.kiji.schema.util.ReferenceCountable

object Resources {
  def doAndRelease[T, R <: ReferenceCountable[R]](resource: => R)(fn: R => T): T = {
    def after(r: R) { r.release() }
    doAnd(resource, after)(fn)
  }

  final case class CompoundException(msg: String, errors: Seq[Exception]) extends Exception

  def doAnd[T, R](resource: => R, after: R => Unit)(fn: R => T): T = {
    var error: Option[Exception] = None

    // Build the resource.
    val res: R = resource
    try fn(res)
    catch {
      // Store the exception in case close fails.
      case e: Exception => {
        error = Some(e)
        throw e
      }
    } finally {
      try after(res)
      catch {
        // Throw the exception(s).
        case e: Exception => {
          error match {
            case Some(firstErr) => throw CompoundException("Exception was thrown while cleaning up "
              + "resources after another exception was thrown.", Seq(firstErr, e))
            case None => throw e
          }
        }
      }
    }
  }
}
