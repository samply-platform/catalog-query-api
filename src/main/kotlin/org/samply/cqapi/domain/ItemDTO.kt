package org.samply.cqapi.domain

import org.samply.catalog.api.domain.model.Category

data class ItemDTO(val id: ItemId,
                   val sellerId: SellerId,
                   val title: String,
                   val description: String,
                   val price: String,
                   val category: Category)
