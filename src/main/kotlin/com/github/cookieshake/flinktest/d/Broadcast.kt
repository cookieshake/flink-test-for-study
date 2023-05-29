package com.github.cookieshake.flinktest.d

import com.github.cookieshake.flinktest.util.*
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.createLocalEnvironment(3)
    val source = env.addSource(ZagaSource())
    val command = env.addSource(UserInputSource())

    val commandStateDescriptor = MapStateDescriptor("command", String::class.java, String::class.java)
    val broadcastCommand = command.broadcast().broadcast(commandStateDescriptor)

    source
        .connect(broadcastCommand)
        .process(object : BroadcastProcessFunction<ZagaEvent, String, ZagaEvent>() {
            override fun processBroadcastElement(value: String, ctx: Context, out: Collector<ZagaEvent>) {
                val state = ctx.getBroadcastState(commandStateDescriptor)
                state.put("prefix", value)
                println("${runtimeContext.indexOfThisSubtask}> 이제 ${value}로 시작하는 이벤트만 필터링합니다!")
            }

            override fun processElement(value: ZagaEvent, ctx: ReadOnlyContext, out: Collector<ZagaEvent>) {
                val prefix = ctx.getBroadcastState(commandStateDescriptor).get("prefix") ?: ""
                if (value.event.startsWith(prefix)) {
                    out.collect(value)
                }
            }
        })
        .addSink(ColoredPrintSink())

    env.execute()
}