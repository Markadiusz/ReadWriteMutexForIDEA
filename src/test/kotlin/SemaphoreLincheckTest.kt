/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */
@file:Suppress("unused")

package kotlinx.coroutines.lincheck

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.*
import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.*
import org.jetbrains.kotlinx.lincheck.verifier.*
import kotlin.reflect.*

abstract class SemaphoreLincheckTestBase(
    private val semaphore: Semaphore, private val seqSpec: KClass<*>
) : AbstractLincheckTest() {
    @Operation
    fun tryAcquire() = this.semaphore.tryAcquire()

    @Operation(promptCancellation = false, allowExtraSuspension = true)
    suspend fun acquire() = this.semaphore.acquire()

    @Operation
    fun release() = this.semaphore.release()

    override fun <O : Options<O, *>> O.customize(): O =
        actorsBefore(0).sequentialSpecification(seqSpec.java)

    override fun ModelCheckingOptions.customize() = checkObstructionFreedom()
}

open class SemaphoreSequential(
    private val permits: Int, private val boundMaxPermits: Boolean
) {
    private var availablePermits = permits
    private val waiters = ArrayList<CancellableContinuation<Unit>>()

    open fun tryAcquire() = tryAcquireImpl()

    private fun tryAcquireImpl(): Boolean {
        if (availablePermits <= 0) return false
        availablePermits--
        return true
    }

    suspend fun acquire() {
        if (tryAcquireImpl()) return
        availablePermits--
        suspendCancellableCoroutine<Unit> { cont ->
            waiters.add(cont)
        }
    }

    fun release() {
        while (true) {
            if (boundMaxPermits) check(availablePermits < permits)
            availablePermits++
            if (availablePermits > 0) return
            val w = waiters.removeAt(0)
            if (w.tryResume0(Unit) { release() }) return
        }
    }
}
