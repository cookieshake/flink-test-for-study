package com.github.cookieshake.flinktest.util

import com.github.ajalt.mordant.rendering.TextColors
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class ColoredPrintSink<Any> : RichSinkFunction<Any>() {
    override fun invoke(value: Any, context: SinkFunction.Context) {
        val subTask = runtimeContext.indexOfThisSubtask
        val color = when(subTask) {
            0 -> TextColors.brightRed
            1 -> TextColors.green
            2 -> TextColors.blue
            else -> TextColors.black
        }
        System.err.println(color("$subTask> $value"))
    }
}