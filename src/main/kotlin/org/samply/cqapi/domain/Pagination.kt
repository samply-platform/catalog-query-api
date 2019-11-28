package org.samply.cqapi.domain

data class Pagination(var first: Int = 10, var after: ItemId? = null)
