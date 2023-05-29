package com.github.cookieshake.flinktest.util

import java.io.Serializable

data class ZagaEvent(val id: Int, val userId: String, val event: String) : Serializable