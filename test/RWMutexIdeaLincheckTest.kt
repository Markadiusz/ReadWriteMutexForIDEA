/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */
@file:Suppress("unused")

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.paramgen.ThreadIdGen
import kotlin.coroutines.CoroutineContext

class RWMutexIdeaLincheckTest : AbstractLincheckTest() {
    private val m = RWMutexIdea()
    private val readPermits = Array(6) { arrayListOf<ReadPermit>() }
    private val writePermits: Array<WritePermit?> = arrayOfNulls(6)
    private val writeIntentPermits: Array<WriteIntentPermit?> = arrayOfNulls(6)
    private val writeLockAcquired = BooleanArray(6)
    private val intentWriteLockAcquired = BooleanArray(6)
    private var lastReadPermitContext: CoroutineContext? = null

    @Operation(allowExtraSuspension = true)
    suspend fun readLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        CoroutineScope(Dispatchers.Default).launch {
            val readPermit = m.acquireReadPermit(true)
            lastReadPermitContext = coroutineContext
            readPermits[threadId] += readPermit
        }
    }

    @Operation
    fun readUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (readPermits[threadId].isEmpty()) return false
        val readPermit = readPermits[threadId].removeAt(0)
        readPermit.release()
        return true
    }

    @Operation(allowExtraSuspension = true)
    suspend fun writeLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        assert(writePermits[threadId] === null)
        val savedLastReadPermitContext = lastReadPermitContext
        writePermits[threadId] = m.acquireWritePermit()
        assert(savedLastReadPermitContext == null || !savedLastReadPermitContext.isActive)
    }

    @Operation
    fun writeUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (writePermits[threadId] === null) return false
        writePermits[threadId]!!.release()
        writePermits[threadId] = null
        return true
    }

    @Operation(allowExtraSuspension = true)
    suspend fun writeIntentLock(@Param(gen = ThreadIdGen::class) threadId: Int) {
        assert(writeIntentPermits[threadId] === null)
        writeIntentPermits[threadId] = m.acquireWriteIntentPermit()
    }

    @Operation
    fun writeIntentUnlock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (writeIntentPermits[threadId] === null) return false
        writeIntentPermits[threadId]!!.release()
        writeIntentPermits[threadId] = null
        return true
    }

    @Operation(allowExtraSuspension = true)
    suspend fun upgradeWriteIntentToWriteLock(@Param(gen = ThreadIdGen::class) threadId: Int): Boolean {
        if (writeIntentPermits[threadId] === null) return false
        writePermits[threadId] = writeIntentPermits[threadId]!!.acquireWritePermit()
        return true
    }
}