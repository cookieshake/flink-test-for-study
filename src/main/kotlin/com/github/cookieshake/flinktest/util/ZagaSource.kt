package com.github.cookieshake.flinktest.util

import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import java.time.Instant
import kotlin.random.Random

class ZagaSource : SourceFunction<ZagaEvent>, CheckpointedFunction {
    private var isRunning = true
    private var index = 0
    private val users = (1..4).map { "u$it" }
    private val events = listOf("Imp", "Click", "Conv")

    lateinit var indexState: ListState<Int>

    override fun run(ctx: SourceFunction.SourceContext<ZagaEvent>) {
        while (isRunning) {
            val selectedUser = users[Random(index).nextInt(0, users.size)]
            val selectedEvent = events[Random(index).nextInt(0, events.size)]
            ctx.collect(ZagaEvent(id = index, userId = selectedUser, event = selectedEvent))
            index += 1
            try {
                Thread.sleep(1000)
            } catch (_: InterruptedException) {}
        }
    }

    override fun cancel() {
        isRunning = false
    }

    override fun initializeState(context: FunctionInitializationContext) {
        val descriptor = ListStateDescriptor("index", Int::class.java)
        indexState = context.operatorStateStore.getListState(descriptor)
        indexState.get().firstOrNull()?.also { index = it }
    }

    override fun snapshotState(context: FunctionSnapshotContext) {
        indexState.clear()
        indexState.add(index)
    }
}