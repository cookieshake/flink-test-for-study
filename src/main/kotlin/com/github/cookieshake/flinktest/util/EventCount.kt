package com.github.cookieshake.flinktest.util

import java.io.Serializable

data class EventCount(val event: String, var count: Int) : Serializable
