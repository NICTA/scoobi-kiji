package com.nicta.scoobi
package io
package kiji

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

import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.io.Writable

class KijiTableSplit(split: org.kiji.mapreduce.impl.KijiTableSplit = new org.kiji.mapreduce.impl.KijiTableSplit) extends InputSplit with Writable {
  def this() = this(new org.kiji.mapreduce.impl.KijiTableSplit)
  override def getLength = split.getLength
  override def getLocations = split.getLocations

  def getStartRow = split.getStartRow
  def getEndRow   = split.getEndRow

  def write(out: DataOutput) { split.write(out) }
  def readFields(in: DataInput) { split.readFields(in) }
}

