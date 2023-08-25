/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

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
        .invocationsPerIteration(25_000)
        .commonConfiguration()
        .customize()
        .check(this::class)

    @Test
    fun stressTest() = StressOptions()
        .invocationsPerIteration(50_000)
        .commonConfiguration()
        .customize()
        .check(this::class)

    private fun <O : Options<O, *>> O.commonConfiguration(): O = this
        .iterations(1000)
        .actorsBefore(0)
        .threads(3)
        .actorsPerThread(3)
        .actorsAfter(0)
        .customize()
}
