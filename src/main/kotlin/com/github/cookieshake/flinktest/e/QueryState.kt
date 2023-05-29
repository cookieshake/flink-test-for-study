package com.github.cookieshake.flinktest.e

import com.github.cookieshake.flinktest.util.*
import org.apache.flink.api.common.functions.RichMapFunction

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.QueryableStateOptions
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.createLocalEnvironment(3)
    env.enableCheckpointing(10000)
    val source = env.addSource(ZagaSource())
    val keyedStream = source.keyBy { it.event }

    val result = keyedStream.map(object : RichMapFunction<ZagaEvent, EventCount>() {
        private lateinit var countState: ValueState<Int?>

        override fun open(parameters: Configuration) {
            val descriptor = ValueStateDescriptor("count", Int::class.java)
            countState = runtimeContext.getState(descriptor)
        }
        override fun map(value: ZagaEvent): EventCount {
            val count = (countState.value() ?: 0) + 1
            countState.update(count)
            return EventCount(event = value.event, count = count)
        }
    })
    result
        .addSink(ColoredPrintSink())
    env.execute()
}