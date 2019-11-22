package org.samply.cqapi.domain

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class CollectionSerializer<T> constructor(private val inner: Serializer<T>) : Serializer<Collection<T>> {

  override fun serialize(topic: String, data: Collection<T>): ByteArray {
    ByteArrayOutputStream().use { baos ->
      DataOutputStream(baos).use { daos ->
        daos.writeInt(data.size)
        data.forEach {
          val serialized = inner.serialize(topic, it)
          daos.writeInt(serialized.size)
          daos.write(serialized)
        }
      }
      return baos.toByteArray()
    }
  }

}

class CollectionDeserializer<T> constructor(private val inner: Deserializer<T>) : Deserializer<Collection<T>> {

  override fun deserialize(topic: String, data: ByteArray): List<T> {
    DataInputStream(ByteArrayInputStream(data)).use { dis ->
      val listSize = dis.readInt()
      return (0 until listSize).map {
        val bytesData = ByteArray(dis.readInt())
        dis.read(bytesData)
        inner.deserialize(topic, bytesData)
      }
    }
  }

}

fun <T> collectionSerde(inner: Serde<T>): Serde<Collection<T>> = Serdes.serdeFrom(
  CollectionSerializer(inner.serializer()),
  CollectionDeserializer(inner.deserializer())
)
