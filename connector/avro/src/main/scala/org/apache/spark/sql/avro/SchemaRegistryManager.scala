/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.avro

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient,
  MockSchemaRegistryClient,
  SchemaRegistryClient}
import org.apache.avro.Schema


class SchemaRegistryManager (val avroOptions: AvroOptions) {
  private lazy val schemaRegistryClient: SchemaRegistryClient =
    if (avroOptions.useConfluentSchemaRegistry) {
      if (avroOptions.schemaRegistryUrl == "https://mock-sr") {
        SchemaRegistryManager.getMockSchemaRegistryClient
      } else {
        new CachedSchemaRegistryClient(avroOptions.schemaRegistryUrl, 100)
      }
    } else {
      throw new UnsupportedOperationException("SchemaRegistryManager is supported only when" +
        " useConfluentSchemaRegistry is true")
    }

  private val cachedSchemas = new ConcurrentHashMap[Int, Schema]().asScala

  def getSchemaById(schemaId: Int): Schema = {
    if (!cachedSchemas.contains(schemaId)) {
      val schema = schemaRegistryClient.getSchemaById(schemaId).rawSchema().asInstanceOf[Schema]
      cachedSchemas.put(schemaId, schema)
    }
    cachedSchemas(schemaId)
  }

  def getSchemaBySubjectAndVersion(subject: String, version: String): Schema = {
    val (schemaString, schemaId) =
      if (version == "latest") {
        val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
        (schemaMetadata.getSchema, schemaMetadata.getId)
      } else {
        val ver = version.toInt
        val schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, ver)
        (schemaMetadata.getSchema, schemaMetadata.getId)
      }
    val schema = new Schema.Parser().setValidateDefaults(false).parse(schemaString)
    cachedSchemas.put(schemaId, schema)
    schema
  }

  def registerSchema(subjectName: String, schema: Schema): Int = {
    val parsedSchema = new AvroSchema(schema)
    schemaRegistryClient.register(subjectName, parsedSchema)
  }


}

class MockSchemaRegistryClientThreadLocal extends ThreadLocal[SchemaRegistryClient]{
  override def initialValue(): SchemaRegistryClient =
    new MockSchemaRegistryClient().asInstanceOf[SchemaRegistryClient]
}

private object SchemaRegistryManager {
  val mockSchemaRegistryClient = new MockSchemaRegistryClientThreadLocal

  private def getMockSchemaRegistryClient: SchemaRegistryClient =
    mockSchemaRegistryClient.get()
}
