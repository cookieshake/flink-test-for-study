package com.github.cookieshake.flinktest.c

import com.github.cookieshake.flinktest.util.*
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.lang.RuntimeException
import kotlin.random.Random


fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.createLocalEnvironment(3)
    env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE)

    val source = env.addSource(ZagaSource()).keyBy { it.event }
    source.map(object : RichMapFunction<ZagaEvent, Map<String, Int>>(), ListCheckpointed<Pair<String, Int>> {
        private val counts: MutableMap<String, Int> = mutableMapOf()

        override fun map(value: ZagaEvent): Map<String, Int> {
            /*
            if (Random.nextInt(0, 10) == 0) {
                println("랜덤에러!")
                throw RuntimeException()
            }
             */
            counts[value.event] = counts.getOrDefault(value.event, 0) + 1
            return counts.toSortedMap()
        }

        override fun snapshotState(checkpointId: Long, timestamp: Long): MutableList<Pair<String, Int>> {
            val newCheckpoint = counts.map { (k, v) -> k to v }.sortedBy { it.first }.toMutableList()
            println("${runtimeContext.indexOfThisSubtask}> 체크포인트중! $newCheckpoint")
            return newCheckpoint
        }

        override fun restoreState(state: MutableList<Pair<String, Int>>) {
            println("${runtimeContext.indexOfThisSubtask}> 체크포인트 복구중! $state")
            counts.clear()
            state.forEach {
                (event, count) -> counts[event] = counts.getOrDefault(event, 0) + count
            }
        }

    })
    .addSink(ColoredPrintSink())

    env.execute("list-checkpoint")
}