/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */
@file:Suppress("unused")
@file:OptIn(ExperimentalCoroutinesApi::class)

package rwmutex

import kotlinx.coroutines.*
import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.paramgen.*
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.*
import org.jetbrains.kotlinx.lincheck.verifier.*

class ReadWriteMutexIdeaLincheckTest : AbstractLincheckTest() {
    private val m = ReadWriteMutexIdeaImpl()
    private val readLockAcquired = IntArray(6)
    private val writeLockAcquired = BooleanArray(6)
    private val intentWriteLockAcquired = BooleanArray(6)

    @Operation(allowExtraSuspension = true, promptCancellation = false)
    suspend fun writeIntentLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        m.writeIntentLock()
        intentWriteLockAcquired[threadId] = true
    }

    @Operation
    fun writeIntentUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!intentWriteLockAcquired[threadId]) return false
        m.writeIntentUnlock()
        intentWriteLockAcquired[threadId] = false
        return true
    }

    @Operation
    suspend fun upgradeWriteIntentToWriteLock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!intentWriteLockAcquired[threadId] || readLockAcquired[threadId] != 0) return false
        m.upgradeWriteIntentToWriteLock()
        intentWriteLockAcquired[threadId] = false
        writeLockAcquired[threadId] = true
        return true
    }

    @Operation(allowExtraSuspension = true, promptCancellation = false)
    suspend fun readLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        m.readLock()
        readLockAcquired[threadId]++
    }

    @Operation
    fun readUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (readLockAcquired[threadId] == 0) return false
        m.readUnlock()
        readLockAcquired[threadId]--
        return true
    }

    //@Operation
    fun tryReadLock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!m.tryReadLock()) return false
        readLockAcquired[threadId]++
        return true
    }

    @Operation(allowExtraSuspension = true, promptCancellation = false)
    suspend fun writeLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        m.writeLock()
        assert(!writeLockAcquired[threadId]) {
            "The mutex is not reentrant, this `writeLock()` invocation had to suspend"
        }
        writeLockAcquired[threadId] = true
    }

    @Operation
    fun writeUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!writeLockAcquired[threadId]) return false
        m.writeUnlock()
        writeLockAcquired[threadId] = false
        return true
    }

    //@Operation
    fun tryWriteLock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!m.tryLock()) return false
        writeLockAcquired[threadId] = true
        return true
    }

    @StateRepresentation
    fun stateRepresentation() = m.stateRepresentation

    override fun <O : Options<O, *>> O.customize() =
        actorsBefore(0)
            .actorsAfter(0)
            .sequentialSpecification(ReadWriteMutexIdeaLincheckTestSequential::class.java)

    override fun ModelCheckingOptions.customize() =
        checkObstructionFreedom(false)
}

class ReadWriteMutexIdeaLincheckTestSequential {
    private val m = ReadWriteMutexIdeaSequential()
    private val readLockAcquired = IntArray(6)
    private val writeLockAcquired = BooleanArray(6)
    private val intentWriteLockAcquired = BooleanArray(6)

    suspend fun writeIntentLock(threadId: Int) {
        m.writeIntentLock()
        intentWriteLockAcquired[threadId] = true
    }

    fun writeIntentUnlock(threadId: Int): Boolean {
        if (!intentWriteLockAcquired[threadId]) return false
        m.writeIntentUnlock()
        intentWriteLockAcquired[threadId] = false
        return true
    }

    suspend fun upgradeWriteIntentToWriteLock(threadId: Int): Boolean {
        if (!intentWriteLockAcquired[threadId] || readLockAcquired[threadId] != 0) return false
        m.upgradeWriteIntentToWriteLock()
        intentWriteLockAcquired[threadId] = false
        writeLockAcquired[threadId] = true
        return true
    }

    fun tryReadLock(threadId: Int): Boolean =
        m.tryReadLock().also { success ->
            if (success) readLockAcquired[threadId]++
        }

    suspend fun readLock(threadId: Int) {
        m.readLock()
        readLockAcquired[threadId]++
    }

    fun readUnlock(threadId: Int): Boolean {
        if (readLockAcquired[threadId] == 0) return false
        m.readUnlock()
        readLockAcquired[threadId]--
        return true
    }

    fun tryWriteLock(threadId: Int): Boolean =
        m.tryWriteLock().also { success ->
            if (success) writeLockAcquired[threadId] = true
        }

    suspend fun writeLock(threadId: Int) {
        m.writeLock()
        writeLockAcquired[threadId] = true
    }

    fun writeUnlock(threadId: Int): Boolean {
        if (!writeLockAcquired[threadId]) return false
        m.writeUnlock()
        writeLockAcquired[threadId] = false
        return true
    }
}

internal class ReadWriteMutexIdeaSequential {
    private var ar = 0
    private var wla = false
    private val wr = ArrayList<CancellableContinuation<Unit>>()
    private val ww = ArrayList<Pair<CancellableContinuation<Unit>, Boolean>>() // (cont, isIntentLock)
    private var iwla = false

    private fun tryResumingReaders() {
        if (!wla && !iwla && ww.isNotEmpty() && ww[0].second) {
            iwla = true
            val w = ww.removeAt(0).first
            w.resume(Unit) { writeIntentUnlock() }
        }
        if (!wla && ww.isEmpty()) {
            ar += wr.size
            wr.forEach { it.resume(Unit) { readUnlock() } }
            wr.clear()
        }
    }

    private fun resumeWriter() {
        check(ww.isNotEmpty() && !ww[0].second)
        val w = ww.removeAt(0).first
        w.resume(Unit) { writeUnlock() }
    }

    suspend fun writeIntentLock() {
        if (wla || iwla || ww.isNotEmpty()) {
            suspendCancellableCoroutine<Unit> { cont ->
                ww += Pair(cont, true)
                cont.invokeOnCancellation {
                    ww -= Pair(cont, true)
                    tryResumingReaders()
                }
            }
        } else {
            iwla = true
        }
    }

    fun writeIntentUnlock() {
        iwla = false
        if (ar == 0 && ww.isNotEmpty() && !ww[0].second) {
            wla = true
            resumeWriter()
        }
        else {
            tryResumingReaders()
        }
    }

    suspend fun upgradeWriteIntentToWriteLock() {
        iwla = false
        if (ar > 0) {
            suspendCancellableCoroutine<Unit> { cont ->
                ww.add(0, Pair(cont, false))
                cont.invokeOnCancellation {
                    ww -= Pair(cont, false)
                    tryResumingReaders()
                }
            }
        }
        else {
            wla = true
        }
    }

    fun tryReadLock(): Boolean {
        if (wla || ww.isNotEmpty()) return false
        ar++
        return true
    }

    suspend fun readLock() {
        if (wla || ww.isNotEmpty()) {
            suspendCancellableCoroutine<Unit> { cont ->
                wr += cont
                cont.invokeOnCancellation { wr -= cont }
            }
        } else {
            ar++
        }
    }

    fun readUnlock() {
        ar--
        if (ar == 0 && !iwla && ww.isNotEmpty()) {
            wla = true
            resumeWriter()
        }
    }

    fun tryWriteLock(): Boolean {
        if (wla || iwla || ar > 0) return false
        wla = true
        return true
    }

    suspend fun writeLock() {
        if (wla || iwla || ar > 0) {
            suspendCancellableCoroutine<Unit> { cont ->
                ww += Pair(cont, false)
                cont.invokeOnCancellation {
                    ww -= Pair(cont, false)
                    tryResumingReaders()
                }
            }
        } else {
            wla = true
        }
    }

    fun writeUnlock() {
        if (ww.isNotEmpty() && !ww[0].second) {
            resumeWriter()
        }
        else {
            wla = false
            tryResumingReaders()
        }
    }
}

// This is an additional test to check the [ReadWriteMutexIdea] synchronization contract.
internal class ReadWriteMutexIdeaCounterLincheckTest : AbstractLincheckTest() {
    private val m = ReadWriteMutexIdeaImpl()
    private var c = 0

    @Operation(allowExtraSuspension = true, promptCancellation = false)
    suspend fun inc(): Int = m.write { c++ }

    @Operation(allowExtraSuspension = true, promptCancellation = false)
    suspend fun get(): Int = m.read { c }

    @StateRepresentation
    fun stateRepresentation(): String = "$c + ${m.stateRepresentation}"

    override fun <O : Options<O, *>> O.customize(): O =
        actorsBefore(0).actorsAfter(0).sequentialSpecification(ReadWriteMutexIdeaCounterSequential::class.java)
}

@Suppress("RedundantSuspendModifier")
class ReadWriteMutexIdeaCounterSequential : VerifierState() {
    private var c = 0

    fun incViaTryLock() = c++
    suspend fun inc() = c++
    suspend fun get() = c

    override fun extractState() = c
}