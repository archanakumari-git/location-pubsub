package com.lightbend.location.entity

final case class LocationResponse(zipCode: Int, status: Boolean, userId: String, requestId: String)
