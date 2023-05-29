package com.github.cookieshake.flinktest.a

import com.github.cookieshake.flinktest.util.ColoredPrintSink
import com.github.cookieshake.flinktest.util.ZagaSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.createLocalEnvironment(3)
    val source = env.addSource(ZagaSource())
    source.addSink(ColoredPrintSink())
    env.execute()
}