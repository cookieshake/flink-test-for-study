package com.github.cookieshake.flinktest.b

import com.github.cookieshake.flinktest.util.EventCount
import com.github.cookieshake.flinktest.util.ZagaEvent
import com.github.cookieshake.flinktest.util.ZagaSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.*
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.createLocalEnvironment(3)
    val source = env.addSource(ZagaSource())
    val keyedStream = source.keyBy { it.event }

    val reduce = keyedStream.map(object : RichMapFunction<ZagaEvent, EventCount>() {
        private lateinit var countState: ReducingState<EventCount>

        override fun open(parameters: Configuration) {
            val descriptor = ReducingStateDescriptor(
                "count",
                { i, j -> i?.let { it.copy(count=it.count+1) } ?: j },
                EventCount::class.java
            )
            countState = runtimeContext.getReducingState(descriptor)

        }
        override fun map(value: ZagaEvent): EventCount {
            countState.add(EventCount(event = value.event, count = 1))
            return countState.get()
        }
    })

    val aggregate = keyedStream.map(object : RichMapFunction<ZagaEvent, EventCount>() {
        private lateinit var countState: AggregatingState<ZagaEvent, EventCount>

        private val aggregator = object : AggregateFunction<ZagaEvent, EventCount, EventCount> {
            override fun createAccumulator(): EventCount? {
                return null
            }

            override fun merge(a: EventCount?, b: EventCount?): EventCount? {
                return if (a == null) { return b!! }
                    else if (b == null) { return a }
                    else { EventCount(event = a.event, count = a.count + b.count) }
            }

            override fun getResult(accumulator: EventCount?): EventCount? {
                return accumulator
            }

            override fun add(value: ZagaEvent?, accumulator: EventCount?): EventCount? {
                val newCount = (accumulator?.count ?: 0) + 1
                return EventCount(event = value!!.event, count = newCount)
            }
        }

        override fun open(parameters: Configuration) {
            val descriptor = AggregatingStateDescriptor(
                "count",
                aggregator,
                EventCount::class.java
            )
            countState = runtimeContext.getAggregatingState(descriptor)

        }

        override fun map(value: ZagaEvent?): EventCount {
            countState.add(value)
            return countState.get()
        }
    })
    env.execute()
}