package org.samply.cqapi.domain

import org.apache.kafka.streams.KafkaStreams
import org.samply.catalog.api.domain.model.Category
import org.samply.catalog.api.domain.model.Item
import java.math.BigDecimal
import java.text.NumberFormat
import java.util.*
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class ItemsQueryService @Inject constructor(private val kafkaStreams: KafkaStreams) {

  suspend fun findById(id: ItemId): ItemDTO? {
    return ItemDTO(
      id,
      "title",
      "description",
      NumberFormat.getInstance(Locale.GERMANY).format(BigDecimal(9.99)),
      Category.A
    )
  }

}
