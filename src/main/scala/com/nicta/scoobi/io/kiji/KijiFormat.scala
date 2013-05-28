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
package io
package kiji

import scala.collection.JavaConverters._
import org.apache.avro.generic.IndexedRecord
import avro.AvroFixed
import avro.AvroSchema
import org.apache.avro.generic.GenericData

/**
 * The KijiFormat type class translates types coming from/going to HBase from/to Scala types
 */
trait KijiFormat[T] {
  def toKiji(t: T): Any
  def fromKiji(t: Any): T
}

/**
 * Definitions of Kiji Formats for main types
 */
object KijiFormat {

  implicit def kijiFormat[A : KijiFormat] = implicitly[KijiFormat[A]]

  implicit def longKijiFormat: KijiFormat[Long] = new KijiFormat[Long] {
    def toKiji(t: Long) = t
    def fromKiji(t: Any) = t.asInstanceOf[Long]
  }
  implicit def intKijiFormat: KijiFormat[Int] = new KijiFormat[Int] {
    def toKiji(t: Int) = t
    def fromKiji(t: Any) = t.asInstanceOf[Int]
  }
  implicit def floatKijiFormat: KijiFormat[Float] = new KijiFormat[Float] {
    def toKiji(t: Float) = t
    def fromKiji(t: Any) = t.asInstanceOf[Float]
  }
  implicit def doubleKijiFormat: KijiFormat[Double] = new KijiFormat[Double] {
    def toKiji(t: Double) = t
    def fromKiji(t: Any) = t.asInstanceOf[Double]
  }
  implicit def stringKijiFormat: KijiFormat[String] = new KijiFormat[String] {
    def toKiji(t: String) = t
    def fromKiji(t: Any) = t.asInstanceOf[CharSequence].toString
  }
  implicit def traversableKijiFormat[T : KijiFormat]: KijiFormat[Traversable[T]] = new KijiFormat[Traversable[T]] {
    def toKiji(t: Traversable[T]) = t.toList.map(implicitly[KijiFormat[T]].toKiji).asJava
    def fromKiji(t: Any) = t.asInstanceOf[java.util.List[T]].asScala.map(implicitly[KijiFormat[T]].fromKiji).toTraversable
  }
  implicit def mapKijiFormat[T : KijiFormat]: KijiFormat[Map[String, T]] = new KijiFormat[Map[String, T]] {
    def toKiji(t: Map[String, T]) = t.mapValues(implicitly[KijiFormat[T]].toKiji).asJava
    def fromKiji(t: Any) = t.asInstanceOf[java.util.Map[CharSequence, T]].asScala.map { case (k, v) => (k.toString, implicitly[KijiFormat[T]].fromKiji(v)) }.toMap
  }
  implicit def avroIndexedRecordKijiFormat[T <: IndexedRecord]: KijiFormat[T] = new KijiFormat[T] {
    def toKiji(t: T) = t
    def fromKiji(t: Any) = t.asInstanceOf[T]
  }
  implicit def avroFixedKijiFormat[T : AvroFixed]: KijiFormat[T] = new KijiFormat[T] {
    def toKiji(t: T) = {
      val array = implicitly[AvroFixed[T]].toArray(t)
      java.nio.ByteBuffer.allocate(array.length).put(array)
    }
    def fromKiji(t: Any) = implicitly[AvroFixed[T]].fromArray(t.asInstanceOf[java.nio.ByteBuffer].array)
  }
  implicit def avroSchemaKijiFormat[T](schema : AvroSchema[T])(implicit ev: schema.AvroType =:= GenericData.Record): KijiFormat[T] = new KijiFormat[T] {
    def toKiji(t: T) = schema.toAvro(t)
    def fromKiji(t: Any) = schema.fromAvro(t.asInstanceOf[schema.AvroType])
  }
}

