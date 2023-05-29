package com.github.cookieshake.flinktest.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator

inline fun <IN, OUT, reified S> KeyedStream<IN, *>.mapWithState(crossinline func: (IN, S?) -> Pair<OUT, S>): SingleOutputStreamOperator<OUT> {
    val mapFunction = object : RichMapFunction<IN, OUT>() {
        lateinit var state: ValueState<S>

        override fun open(parameters: Configuration) {
            val descriptor = ValueStateDescriptor("state", S::class.java)
            state = runtimeContext.getState(descriptor)
        }

        override fun map(input: IN): OUT {
            val currentState = if (::state.isInitialized) {
                state.value()
            } else {
                null
            }
            val (output, newState) = func(input, currentState)
            state.update(newState)
            return output
        }
    }
    return map(mapFunction)
}