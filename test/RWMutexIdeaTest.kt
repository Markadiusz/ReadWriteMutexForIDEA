import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import kotlin.test.Test

class RWMutexIdeaTest : TestBase() {

    suspend fun suspendForever() = delay(1_000_000_000L)

    @Test
    fun cancelOnAcquiredWritePermitTrue() = runTest {
        val m = RWMutexIdea()

        launch {
            var readPermit: Permit? = null
            try {
                readPermit = m.acquireReadPermit(true)
                expect(1)
                suspendForever()
            } finally {
                expect(3)
                readPermit!!.release()
            }
        }
        yield()

        expect(2)

        m.acquireWritePermit()

        expect(4)

        finish(5)
    }

    @Test
    fun cancelOnAcquiredWritePermitFalse() = runTest {
        val m = RWMutexIdea()

        m.acquireReadPermit(false)

        expect(1)

        val writeJob = launch {
            expect(2)
            m.acquireWritePermit()
            expectUnreached()
        }
        yield()

        expect(3)

        writeJob.cancel()

        finish(4)
    }

    @Test
    fun tryAcquireReadPermitIsNotCancelledByAcquireWritePermit() = runTest {
        val m = RWMutexIdea()

        val readPermit = m.tryAcquireReadPermit()
        check(readPermit !== null)

        expect(1)

        val writeJob = launch {
            expect(2)
            m.acquireWritePermit()
            expectUnreached()
        }
        yield()

        expect(3)

        writeJob.cancel()

        finish(4)
    }

    @Test
    fun acquireWriteIntentIsNotCancelledByAcquireWritePermit() = runTest {
        val m = RWMutexIdea()

        m.acquireWriteIntentPermit()

        expect(1)

        val writeJob = launch {
            expect(2)
            m.acquireWritePermit()
            expectUnreached()
        }
        yield()

        expect(3)

        writeJob.cancel()

        finish(4)
    }

    @Test
    fun tryAcquireReadPermitReturnValue() = runTest {
        val m = RWMutexIdea()

        val readPermit = m.tryAcquireReadPermit()
        check(readPermit !== null)

        readPermit!!.release()

        m.acquireWritePermit()

        val readPermit2 = m.tryAcquireReadPermit()
        check(readPermit2 == null)
    }

    @Test
    fun tryAcquireWriteIntentPermitReturnValue() = runTest {
        val m = RWMutexIdea()

        val writeIntentPermit = m.tryAcquireWriteIntentPermit()
        check(writeIntentPermit !== null)

        writeIntentPermit!!.release()

        m.acquireWritePermit()

        val writeIntentPermit2 = m.tryAcquireWriteIntentPermit()
        check(writeIntentPermit2 == null)
    }

    @Test
    fun tryAcquireWritePermitReturnValue() = runTest {
        val m = RWMutexIdea()

        val writePermit = m.tryAcquireWritePermit()
        check(writePermit !== null)

        writePermit!!.release()

        m.acquireWritePermit()

        val writePermit2 = m.tryAcquireWritePermit()
        check(writePermit2 == null)
    }

    @Test
    fun upgradeWriteIntentCancelsActiveReaders() = runTest {
        val m = RWMutexIdea()

        val writeIntentPermit = m.acquireWriteIntentPermit()

        expect(1)

        launch {
            var readPermit: ReadPermit? = null
            try {
                readPermit = m.acquireReadPermit(true)
                expect(2)
                suspendForever()
            } finally {
                expect(3)
                readPermit!!.release()
            }
        }
        yield()

        val writePermit = writeIntentPermit.acquireWritePermit()
        expect(4)

        writePermit.release()

        finish(5)
    }

    @Test
    fun writeEpochValue() = runTest {
        val m = RWMutexIdea()

        check(m.writeEpoch == 0L)

        val writePermit = m.acquireWritePermit()
        check(m.writeEpoch == 0L)

        writePermit.release()
        check(m.writeEpoch == 1L)

        val writePermit2 = m.acquireWritePermit()
        check(m.writeEpoch == 1L)

        val writeJob = launch {
            val writePermit3 = m.acquireWritePermit()
            // Should be 2 because the writeLock must have been released for us to acquire it.
            check(m.writeEpoch == 2L)

            writePermit3.release()
            check(m.writeEpoch == 3L)
        }
        yield()

        check(m.writeEpoch == 1L)

        writePermit2.release()

        writeJob.join()
        check(m.writeEpoch == 3L)

        val writeIntentPermit = m.acquireWriteIntentPermit()
        check(m.writeEpoch == 3L)

        val writePermit4 = writeIntentPermit.acquireWritePermit()
        check(m.writeEpoch == 3L)

        writePermit4.release()
        check(m.writeEpoch == 4L)

        val readPermit = m.acquireReadPermit(true)
        check(m.writeEpoch == 4L)

        readPermit.release()
        check(m.writeEpoch == 4L)
    }

    @Test
    fun allActiveReadersAreCancelled() = runTest {
        val m = RWMutexIdea()

        repeat(10) {
            launch {
                var readPermit: Permit? = null
                try {
                    readPermit = m.acquireReadPermit(true)
                    suspendForever()
                } finally {
                    readPermit!!.release()
                }
            }
        }
        yield()

        m.acquireWritePermit()
    }
}