/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */
package rwmutex

import kotlinx.coroutines.*
import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.*
import org.jetbrains.kotlinx.lincheck.strategy.stress.*
import org.junit.jupiter.api.*

abstract class AbstractLincheckTest {
    open fun <O : Options<O, *>> O.customize(): O = this
    open fun ModelCheckingOptions.customize(): ModelCheckingOptions = this
    open fun StressOptions.customize(): StressOptions = this

    @Test
    fun modelCheckingTest() = ModelCheckingOptions()
        .invocationsPerIteration(10_000)
        .commonConfiguration()
        .customize()
        .check(this::class)

//    @Test
    fun stressTest() = StressOptions()
        .invocationsPerIteration(10_000)
        .commonConfiguration()
        .customize()
        .check(this::class)

    private fun <O : Options<O, *>> O.commonConfiguration(): O = this
        .iterations(100)
        .actorsBefore(2)
        .threads(3)
        .actorsPerThread(3)
        .actorsAfter(2)
        .customize()
}

@OptIn(InternalCoroutinesApi::class)
fun <T> CancellableContinuation<T>.tryResume0(value: T, onCancellation: (Throwable?) -> Unit): Boolean {
    tryResume(value, null, onCancellation).let {
        if (it == null) return false
        completeResume(it)
        return true
    }
}