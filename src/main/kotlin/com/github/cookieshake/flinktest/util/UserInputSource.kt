package com.github.cookieshake.flinktest.util

import org.apache.flink.streaming.api.functions.source.SourceFunction

class UserInputSource : SourceFunction<String> {
    private var isRunning = true

    override fun run(ctx: SourceFunction.SourceContext<String>) {
        while (isRunning) {
            ctx.collect(readln())
        }
    }

    override fun cancel() {
        isRunning = false
    }
}