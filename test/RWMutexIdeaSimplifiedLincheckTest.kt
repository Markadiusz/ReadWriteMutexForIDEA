/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */
@file:Suppress("unused")
@file:OptIn(ExperimentalCoroutinesApi::class)

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.suspendCancellableCoroutine
import org.jetbrains.kotlinx.lincheck.Options
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.paramgen.ThreadIdGen
import kotlin.coroutines.resume

class RWMutexIdeaSimplifiedLincheckTest : AbstractLincheckTest() {
    private val m = RWMutexIdeaSimplified()
    private val readLockAcquired = IntArray(6)
    private val writeLockAcquired = BooleanArray(6)
    private val intentWriteLockAcquired = BooleanArray(6)

    @Operation(allowExtraSuspension = true)
    suspend fun readLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        m.acquireReadPermit()
        readLockAcquired[threadId]++
    }

    @Operation
    fun readUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (readLockAcquired[threadId] == 0) return false
        m.releaseReadPermit()
        readLockAcquired[threadId]--
        return true
    }

    @Operation(allowExtraSuspension = true)
    suspend fun writeLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        m.acquireWritePermit()
        assert(!writeLockAcquired[threadId]) {
            "The mutex is not reentrant, this `writeLock()` invocation had to suspend"
        }
        writeLockAcquired[threadId] = true
    }

    @Operation
    fun writeUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!writeLockAcquired[threadId]) return false
        m.releaseWritePermit()
        writeLockAcquired[threadId] = false
        return true
    }

    @Operation(allowExtraSuspension = true)
    suspend fun writeIntentLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        m.acquireWriteIntentPermit()
        intentWriteLockAcquired[threadId] = true
    }

    @Operation
    fun writeIntentUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!intentWriteLockAcquired[threadId]) return false
        m.releaseWriteIntentPermit()
        intentWriteLockAcquired[threadId] = false
        return true
    }

    @Operation(allowExtraSuspension = true)
    suspend fun upgradeWriteIntentToWriteLock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (!intentWriteLockAcquired[threadId] || readLockAcquired[threadId] != 0) return false
        intentWriteLockAcquired[threadId] = false
        m.upgradeWriteIntentToWrite()
        writeLockAcquired[threadId] = true
        return true
    }

    override fun <O : Options<O, *>> O.customize() =
        sequentialSpecification(RWMutexIdeaSimplifiedLincheckTestSequential::class.java)
}

class RWMutexIdeaSimplifiedLincheckTestSequential {
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
        intentWriteLockAcquired[threadId] = false
        m.upgradeWriteIntentToWriteLock()
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
            w.resume(Unit) { writeIntentUnlock() }
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
        w.resume(Unit) { writeUnlock() }
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

    fun writeIntentUnlock() {
        iwla = false
        // Resume a writer if there are no readers.
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
                    tryResumeReadersAndFirstWriteIntent()
                }
            }
        } else {
            // We are free to acquire the write lock.
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

    fun writeUnlock() {
        // Are there waiting writers?
        if (ww.isNotEmpty() && !iwla) {
            resumeWriter()
        } else {
            wla = false
            tryResumeReadersAndFirstWriteIntent()
        }
    }
}