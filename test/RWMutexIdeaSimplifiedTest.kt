/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import kotlin.test.Test

class RWMutexIdeaSimplifiedTest : TestBase() {

    @Test
    fun writeAcquireRelease() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireWritePermit()
        m.releaseWritePermit()

        m.acquireWritePermit()
        m.releaseWritePermit()
    }

    @Test
    fun readAcquireRelease() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireReadPermit()
        m.releaseReadPermit()

        m.acquireWritePermit()
    }

    @Test
    fun writeIntentAcquireRelease() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireWriteIntentPermit()
        m.releaseWriteIntentPermit()

        m.acquireWriteIntentPermit()
        m.releaseWriteIntentPermit()

        m.acquireWritePermit()
    }

    @Test
    fun parallelReadersAllowed() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireReadPermit()
        m.acquireReadPermit()
        m.acquireWriteIntentPermit()

        m.releaseReadPermit()
        m.releaseReadPermit()
        m.releaseWriteIntentPermit()

        m.acquireWritePermit()
    }

    @Test
    fun writePermitIsExclusive() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireWritePermit()
        expect(1)

        val wJob = launch {
            expect(2)
            m.acquireWritePermit()
            expectUnreached()
        }
        yield()

        expect(3)
        wJob.cancel()

        finish(4)
    }

    @Test
    fun noParallelWriteIntentPermits() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireWriteIntentPermit()
        expect(1)

        val wiJob = launch {
            expect(2)
            m.acquireWriteIntentPermit()
            expectUnreached()
        }
        yield()

        expect(3)
        wiJob.cancel()

        finish(4)
    }

    @Test
    fun nextWriteIntentIsResumedWhileReadersAreActive() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireReadPermit()

        m.acquireWriteIntentPermit()
        m.releaseWriteIntentPermit()

        m.acquireWriteIntentPermit()
    }

    @Test
    fun writeLockUpgradedFromWriteIntentReleasesBackToWriteIntent() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireWriteIntentPermit()
        m.upgradeWriteIntentToWrite()

        m.releaseWritePermit()

        val writeJob = launch {
            expect(1)
            m.acquireWritePermit()
            expectUnreached()
        }

        // Switch to `writeJob`
        yield()

        writeJob.cancel()
        finish(2)
    }

    @Test
    fun writersFormAQueue() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireWriteIntentPermit()

        val writeJob1 = launch {
            expect(1)
            m.acquireWritePermit()
            expect(5)
            m.releaseWritePermit()
        }
        yield()

        val writeJob2 = launch {
            expect(2)
            m.acquireWritePermit()
            expect(6)
            m.releaseWritePermit()
        }
        yield()

        val writeJob3 = launch {
            expect(3)
            m.acquireWritePermit()
            expect(7)
            m.releaseWritePermit()
        }
        yield()

        expect(4)

        m.releaseWriteIntentPermit()

        writeJob1.join()
        writeJob2.join()
        writeJob3.join()

        finish(8)
    }

    @Test
    fun writeIntentsFormAQueue() = runTest {
        val m = RWMutexIdeaSimplified()

        m.acquireWritePermit()

        val writeIntentJob1 = launch {
            expect(1)
            m.acquireWriteIntentPermit()
            expect(5)
            m.releaseWriteIntentPermit()
        }
        yield()

        val writeIntentJob2 = launch {
            expect(2)
            m.acquireWriteIntentPermit()
            expect(6)
            m.releaseWriteIntentPermit()
        }
        yield()

        val writeIntentJob3 = launch {
            expect(3)
            m.acquireWriteIntentPermit()
            expect(7)
            m.releaseWriteIntentPermit()
        }
        yield()

        expect(4)

        m.releaseWritePermit()

        writeIntentJob1.join()
        writeIntentJob2.join()
        writeIntentJob3.join()

        finish(8)
    }
}