import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import kotlin.test.Test

class RWMutexIdeaTest : TestBase() {

    @Test
    fun cancelOnAcquiredWritePermitTrue() = runTest {
        val m = RWMutexIdea()

        launch {
            var readPermit: Permit? = null
            try {
                readPermit = m.acquireReadPermit(true)
                expect(1)
                delay(1_000_000L)
            }
            finally {
                expect(3)
                readPermit!!.release()
            }
        }
        yield()

        val writeJob = launch {
            expect(2)
            m.acquireWritePermit()
            expect(4)
        }
        writeJob.join()

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
}