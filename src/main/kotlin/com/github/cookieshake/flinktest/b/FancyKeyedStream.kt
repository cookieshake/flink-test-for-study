package com.github.cookieshake.flinktest.b

import com.github.cookieshake.flinktest.util.ColoredPrintSink
import com.github.cookieshake.flinktest.util.EventCount
import com.github.cookieshake.flinktest.util.ZagaSource
import com.github.cookieshake.flinktest.util.mapWithState
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.createLocalEnvironment(3)
    val source = env.addSource(ZagaSource())
    val keyedStream = source.keyBy { it.event }

    keyedStream.mapWithState { zaga, s: Int? ->
        val count: Int = s ?: 0
        val newCount = count + 1
        EventCount(event = zaga.event, newCount) to newCount
    }
    .addSink(ColoredPrintSink())

    env.execute()
}