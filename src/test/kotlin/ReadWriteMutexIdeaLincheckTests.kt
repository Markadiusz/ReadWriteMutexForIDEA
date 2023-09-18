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
import rwmutex.ReadWriteMutexIdeaImpl.UnlockPolicy.*
import kotlin.coroutines.*
import org.junit.jupiter.api.*
import kotlin.reflect.jvm.*

class ReadWriteMutexIdeaLincheckTest : AbstractLincheckTest() {
    private val m = ReadWriteMutexIdeaImpl()
    private val readLockAcquired = IntArray(6)
    private val writeLockAcquired = BooleanArray(6)
    private val intentWriteLockAcquired = BooleanArray(6)

    @Operation(allowExtraSuspension = true, promptCancellation = false)
    suspend fun writeIntentLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        m.writeIntentLock()
        assert(!intentWriteLockAcquired[threadId]) {
            "The mutex is not reentrant, this `writeIntentLock()` invocation had to suspend"
        }
        intentWriteLockAcquired[threadId] = true
    }

    @Operation
    fun writeIntentUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!intentWriteLockAcquired[threadId]) return false
        m.writeIntentUnlock(PRIORITIZE_WRITERS)
        intentWriteLockAcquired[threadId] = false
        return true
    }

    @Operation(allowExtraSuspension = true)
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
        m.writeUnlock(PRIORITIZE_WRITERS)
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

    //@Test
    fun customModelCheckingTest() = ModelCheckingOptions()
        .invocationsPerIteration(50_000)
        .iterations(500)
        .addCustomScenario {
            parallel {
                thread {
                    actor(::writeLock, 1)
                    //Actor(::writeLock.javaMethod!!, listOf(1), false, true)
                }
                thread {
                    actor(::readLock, 2)
                    actor(::writeIntentLock, 2)
                }
                thread {
                    actor(::writeIntentLock, 3)
                    actor(::upgradeWriteIntentToWriteLock, 3)
                }
            }
        }
        .checkObstructionFreedom(false)
        .sequentialSpecification(ReadWriteMutexIdeaLincheckTestSequential::class.java)
        .check(this::class)
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
        m.writeIntentUnlock(true)
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
        m.writeUnlock(true)
        writeLockAcquired[threadId] = false
        return true
    }
}

internal class ReadWriteMutexIdeaSequential {
    private var ar = 0
    private var wla = false
    private var iwla = false
    private val wr = ArrayList<CancellableContinuation<Unit>>()
    private val ww = ArrayList<CancellableContinuation<Unit>>()
    private val wi = ArrayList<CancellableContinuation<Unit>>()

    // Stores a thread that suspended during a upgradeWriteIntentToWriteLock call.
    // iwla is set to true when upgradingThread isn't null.
    private var upgradingThread: CancellableContinuation<Unit>? = null

    // 'Readers' refers to normal readers and to threads that called writeIntent, but haven't upgraded to a writer yet.
    private fun tryResumeReadersAndFirstWriteIntent() {
        // Resumes a waiting writeIntentLock
        // if neither of the write(Intent) locks is acquired and there are no waiting writers.
        if (!wla && !iwla && ww.isEmpty() && wi.isNotEmpty()) {
            iwla = true
            val w = wi.removeAt(0)
            w.resume(Unit) { writeIntentUnlock(false) }
        }
        // Resumes waiting readers if the write lock isn't acquired,
        // there are no waiting writers and there isn't an upgrading thread.
        if (!wla && ww.isEmpty() && upgradingThread === null) {
            ar += wr.size
            wr.forEach { it.resume(Unit) { readUnlock() } }
            wr.clear()
        }
    }

    private fun resumeWriter() {
        // Resumes a waiting writer.
        check(ww.isNotEmpty())
        val w = ww.removeAt(0)
        w.resume(Unit) { writeUnlock(true) }
    }

    suspend fun writeIntentLock() {
        // Is either of the write(Intent) locks acquired or are there waiting writers?
        if (wla || iwla || ww.isNotEmpty()) {
            suspendCancellableCoroutine { cont ->
                wi += cont
                cont.invokeOnCancellation { wi -= cont }
            }
        } else {
            // We are free to acquire the writeIntent lock.
            iwla = true
        }
    }

    fun writeIntentUnlock(prioritizeWriters: Boolean) {
        iwla = false
        // Resume a writer if there are no readers.
        //if (ar == 0 && ww.isNotEmpty() && (prioritizeWriters || wi.isEmpty())) {
        if (ar == 0 && ww.isNotEmpty()) {
            wla = true
            resumeWriter()
        } else {
            tryResumeReadersAndFirstWriteIntent()
        }
    }

    suspend fun upgradeWriteIntentToWriteLock() {
        if (ar > 0) {
            // Wait until all active readers finish.
            suspendCancellableCoroutine<Unit> { cont ->
                upgradingThread = cont
                cont.invokeOnCancellation {
                    upgradingThread = null
                    //iwla = false
                    tryResumeReadersAndFirstWriteIntent()
                }
            }
        } else {
            // We are free to acquire the write lock.
            iwla = false
            wla = true
        }
    }

    fun tryReadLock(): Boolean {
        // Are there active or waiting writers or a thread upgrading to a writer?
        if (wla || ww.isNotEmpty() || upgradingThread != null) return false
        // We are free to acquire the read lock.
        ar++
        return true
    }

    suspend fun readLock() {
        // Are there active or waiting writers or a thread upgrading to a writer?
        if (wla || ww.isNotEmpty() || upgradingThread != null) {
            suspendCancellableCoroutine<Unit> { cont ->
                wr += cont
                cont.invokeOnCancellation { wr -= cont }
            }
        } else {
            // We are free to acquire the read lock.
            ar++
        }
    }

    fun readUnlock() {
        ar--
        if (ar == 0) {
            // Is there a "write-intent" lock upgrading to the "write" lock?
            if (upgradingThread != null) {
                iwla = false
                wla = true
                val cont: CancellableContinuation<Unit> = upgradingThread!!
                upgradingThread = null
                cont.resume(Unit)
            } else if (!iwla && ww.isNotEmpty()) {
                wla = true
                resumeWriter()
            }
            // If there is no upgrading thread and there are no waiting writers,
            // then there can't be any waiting writeIntents, so we don't need to resume them.
        }
    }

    fun tryWriteLock(): Boolean {
        // Is either of the write(Intent) locks is acquired or are there active readers?
        if (wla || iwla || ar > 0) return false
        // We are free to acquire the write lock.
        wla = true
        return true
    }

    suspend fun writeLock() {
        // Is either of the write(Intent) locks is acquired or are there active readers?
        if (wla || iwla || ar > 0) {
            suspendCancellableCoroutine { cont ->
                ww += cont
                cont.invokeOnCancellation {
                    ww -= cont
                    // Resumes a waiting writeIntent if iwla is false and we were the last waiting writer.
                    if (ww.isEmpty()) tryResumeReadersAndFirstWriteIntent()
                }
            }
        } else {
            // We are free to acquire the write lock.
            wla = true
        }
    }

    fun writeUnlock(prioritizeWriters: Boolean) {
        // Are there waiting writers?
        //if (ww.isNotEmpty() && (prioritizeWriters || wi.isEmpty())) {
        if (ww.isNotEmpty()) {
            resumeWriter()
        } else {
            wla = false
            tryResumeReadersAndFirstWriteIntent()
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