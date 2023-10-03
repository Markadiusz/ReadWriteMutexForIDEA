import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import kotlin.test.Test

class RWMutexIdeaTest : TestBase() {

    @Test
    fun cancelOnAcquiredWritePermitTrue() = runTest {
        val m = RWMutexIdea()

        val readJob = launch {
            m.acquireReadPermit(true)
            expect(1)
        }
        yield()

        val writeJob = launch {
            m.acquireWritePermit()
            expect(2)
        }
        writeJob.join()

        finish(3)
    }

    @Test
    fun cancelOnAcquiredWritePermitFalse() = runTest {
        val m = RWMutexIdea()
        m.acquireReadPermit(false)

        expect(1)

        val writeJob = launch {
            m.acquireWritePermit()
            expectUnreached()
        }
        yield()

        writeJob.cancel()

        finish(2)
    }
}