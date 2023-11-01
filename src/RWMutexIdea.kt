import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.ReentrantLock
import kotlinx.coroutines.*
import kotlin.concurrent.withLock
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

/**
 * This interface outlines the methods and structure for a specific readers-writer mutex for IntelliJ IDEA.
 * In essence, it supports "read", "write", and "write-intent" locks, prioritizing writers and cancelling
 * coroutines that  are holding "read" locks when acquiring "write" locks.
 */
interface RWMutexIdea {
    /**
     * Acquires a "read" permit, allowing for concurrent read access.
     * This method may suspend until a permit is available.
     * If [cancelOnAcquireWritePermit] is set, this method must be called in such a manner
     * that the [ReadPermit] is released upon the coroutine's cancellation
     * (i.e. inside a try { ... } finally { ... } block).
     *
     * @param cancelOnAcquireWritePermit indicates whether the current coroutine
     * is cancelled upon the acquisition of a [WritePermit]
     *
     * @return a [ReadPermit] which can be released after use.
     */
    suspend fun acquireReadPermit(cancelOnAcquireWritePermit: Boolean): ReadPermit

    /**
     * Tries to acquire a "read" permit, allowing for concurrent read access,
     * without suspension.
     *
     * @return a [ReadPermit] which can be released after use,
     * or `null` if this attempt has failed.
     */
    fun tryAcquireReadPermit(): ReadPermit?

    /**
     * Acquires a write permit, ensuring exclusive write access.
     * This method may suspend until a permit is available.
     *
     * @return a [WritePermit] which can be released after use.
     */
    suspend fun acquireWritePermit(): WritePermit

    /**
     * Tries to acquire a write permit, ensuring exclusive write access,
     * without suspension.
     *
     * @return a [WritePermit] which can be released after use,
     * or `null` if this attempt has failed.
     */
    fun tryAcquireWritePermit(): WritePermit?

    /**
     * Acquires a write-intent permit. This represents an intent to acquire a write permit in the future.
     * This allows potential optimizations where the transition from a read lock to a write lock can be smoother.
     * This method may suspend until a permit is available.
     *
     * @return a [WriteIntentPermit] which can be upgraded to a [WritePermit] or released.
     */
    suspend fun acquireWriteIntentPermit(): WriteIntentPermit

    /**
     * Tries to acquire a write-intent permit without suspension.
     *
     * This represents an intent to acquire a write permit in the future.
     * This allows potential optimizations where the transition from a read lock to a write lock can be smoother.
     *
     * @return a [WriteIntentPermit] which can be upgraded to a [WritePermit],
     * or `null` if this attempt has failed.
     */
    fun tryAcquireWriteIntentPermit(): WriteIntentPermit?

    /**
     * The number of completed "write" actions
     * (more formally, successful [WritePermit.release] calls).
     *
     * Using this information, one can easily implement the
     * "upgrade 'read' permit to the 'write' one or restart" functionality
     * by releasing the holding read permit, acquiring the write one,
     * and checking that the [writeEpoch] has not been changed,
     * restarting in the latter case.
     */
    val writeEpoch: Long
}

/**
 * Creates a new [RWMutexIdea] instance.
 */
fun RWMutexIdea(): RWMutexIdea = RWMutexIdeaImpl()

/**
 * Base sealed interface for permits. Each permit should have a method to release it.
 */
sealed interface Permit {
    /**
     * Releases the acquired permit, potentially allowing other
     * threads/tasks to acquire their own permits.
     */
    fun release()
}

/**
 * Represents a permit acquired for reading.
 * Multiple [ReadPermit]s can be acquired concurrently.
 */
interface ReadPermit : Permit

/**
 * Represents a permit acquired for writing.
 * Ensures exclusive access.
 */
interface WritePermit : Permit

/**
 * Represents a permit indicating the intent to write.
 * Provides functionality to upgrade this permit to a full [WritePermit].
 * Note that at most one [WriteIntentPermit] can be hold simultaneously.
 */
interface WriteIntentPermit : Permit {
    /**
     * Upgrades this [WriteIntentPermit] to a full [WritePermit].
     * This method may suspend until the upgrade completes.
     *
     * Releasing the resulting [WritePermit] downgrades the mutex
     * state back to the "write-intent lock is acquired" one.
     *
     * @return a [WritePermit] which can be released after use,
     */
    suspend fun acquireWritePermit(): WritePermit

    /**
     * Tries to upgrade this [WriteIntentPermit] to
     * a full [WritePermit] without suspension.
     *
     * Releasing the resulting [WritePermit] downgrades the mutex
     * state back to the "write-intent lock is acquired" one.
     *
     * @return a [WritePermit] which can be released after use.
     * or `null` if this attempt has failed.
     */
    fun tryAcquireWritePermit(): WritePermit?
}

private class ReadPermitImpl(
    private val mutex: RWMutexIdeaImpl,
    private val coroutineContextToCancel: CoroutineContext?
) : ReadPermit {
    // false -- "acquired"
    // true  -- "released"
    private val state = atomic(false)

    private val cancellable
        get() =
            coroutineContextToCancel != null

    override fun release() {
        // Ensure that the permit cannot be released multiple times.
        check(state.compareAndSet(false, true)) {
            "This 'read' permit has already been released"
        }
        if (cancellable) {
            // Remove this reader from the list of active ones.
            // It is possible that this reader has already been
            // logically cancelled due to a concurrent race --
            // this is fine :)
            mutex.activeReadersLock.withLock {
                mutex.activeReaders -= this@ReadPermitImpl
            }
        }
        // Release the permit.
        mutex.mutex.releaseReadPermit()
    }

    fun cancel() {
        check(cancellable) { "This permit is not cancellable" }
        coroutineContextToCancel!!.cancel(ReadCancellationException)
    }
}

object ReadCancellationException : CancellationException(
    "The running 'read' operation associated with the acquired `ReadPermit` has been cancelled by `RWMutexIdea`"
) {
    private fun readResolve(): Any = ReadCancellationException
}

private class WritePermitImpl(
    private val mutex: RWMutexIdeaImpl
) : WritePermit {
    // false -- "acquired"
    // true  -- "released"
    private val state = atomic(false)

    override fun release() {
        // Ensure that the permit cannot be released multiple times.
        check(state.compareAndSet(false, true)) {
            "This 'write' permit has already been released"
        }
        // Increment the write epoch.
        val curEpoch = mutex._writeEpoch.value
        mutex._writeEpoch.value = curEpoch + 1
        // Release the write permit.
        mutex.mutex.releaseWritePermit()
    }
}

private class WriteIntentPermitImpl(
    private val mutex: RWMutexIdeaImpl
) : WriteIntentPermit {

    // 0 -- "write-intent" permit acquired
    // 1 -- "write-intent" permit released
    // 2 -- upgraded to "write" permit
    private val state = atomic(0)

    override fun release() {
        // Ensure that the permit cannot be released multiple times
        // or released after `upgradeToWritePermit` is called.
        check(state.compareAndSet(0, 1)) {
            if (state.value == 1) {
                "This 'write-intent' permit has already been released"
            } else { // invoked.value == 2
                "This 'write-intent' permit has already been upgraded to the 'write' one"
            }
        }
        mutex.mutex.releaseWriteIntentPermit()
    }

    override suspend fun acquireWritePermit(): WritePermit {
        // Ensure that upgrading cannot be called multiple times
        // or after this permit has been released.
        check(state.compareAndSet(0, 2)) {
            if (state.value == 1) {
                "This 'write-intent' permit has already been released"
            } else { // invoked.value == 2
                "This 'write-intent' permit has already been upgraded to the 'write' one"
            }
        }
        mutex.cancelActiveReaders()
        mutex.mutex.upgradeWriteIntentToWrite()
        return WritePermitImpl(mutex)
    }

    override fun tryAcquireWritePermit(): WritePermit? {
        // Ensure that upgrading cannot be called multiple times
        // or after this permit has been released.
        check(state.compareAndSet(0, 2)) {
            if (state.value == 1) {
                "This 'write-intent' permit has already been released"
            } else { // invoked.value == 2
                "This 'write-intent' permit has already been upgraded to the 'write' one"
            }
        }
        return if (mutex.mutex.tryUpgradeWriteIntentToWrite()) {
            WritePermitImpl(mutex)
        } else {
            // The attempt has failed; reset the state and finish.
            state.value = 0
            null
        }
    }
}

private class RWMutexIdeaImpl : RWMutexIdea {
    val mutex = RWMutexIdeaSimplified()

    // Maintain a set of active readers to cancel the
    // corresponding coroutines when a "write" request comes.
    // For simplicity, we use coarse-grained locking approach.
    //
    // TODO: this synchronization is incorrect, but it's OK for a prototype.
    val activeReaders: MutableSet<ReadPermitImpl> = HashSet()
    val activeReadersLock = ReentrantLock()

    val _writeEpoch = atomic(0L)

    override val writeEpoch: Long
        get() = _writeEpoch.value

    override suspend fun acquireReadPermit(cancelOnAcquireWritePermit: Boolean): ReadPermit {
        mutex.acquireReadPermit()
        return if (cancelOnAcquireWritePermit) {
            ReadPermitImpl(this, coroutineContext).also {
                activeReadersLock.withLock {
                    activeReaders += it
                }
            }
        } else {
            ReadPermitImpl(this, null)
        }
    }

    override fun tryAcquireReadPermit(): ReadPermit? =
        if (mutex.tryAcquireReadPermit()) {
            ReadPermitImpl(this, null)
        } else {
            null
        }

    override suspend fun acquireWritePermit(): WritePermit {
        cancelActiveReaders()
        mutex.acquireWritePermit()
        return WritePermitImpl(this)
    }

    override fun tryAcquireWritePermit(): WritePermit? =
        if (mutex.tryAcquireWritePermit()) {
            WritePermitImpl(this)
        } else {
            null
        }


    override suspend fun acquireWriteIntentPermit(): WriteIntentPermit {
        mutex.acquireWriteIntentPermit()
        return WriteIntentPermitImpl(this)
    }

    override fun tryAcquireWriteIntentPermit(): WriteIntentPermit? =
        if (mutex.tryAcquireWriteIntentPermit()) {
            WriteIntentPermitImpl(this)
        } else {
            null
        }

    fun cancelActiveReaders() {
        activeReadersLock.withLock {
            activeReaders.forEach { it.cancel() }
            activeReaders.clear()
        }
    }
}

@OptIn(ExperimentalCoroutinesApi::class)
internal class RWMutexIdeaSimplified {
    // For simplicity, coarse-grained synchronization is used.
    private val synchronizationLock = ReentrantLock()

    /**
     * List of suspended coroutines waiting for a "read" permit.
     */
    private val waitingReaders = ArrayDeque<CancellableContinuation<Unit>>()

    /**
     * List of suspended coroutines waiting for the "write" permit.
     */
    private val waitingWriters = ArrayDeque<CancellableContinuation<Unit>>()

    /**
     * List of suspended coroutines waiting for the "write-intent" permit.
     */
    private val waitingWriteIntents = ArrayDeque<CancellableContinuation<Unit>>()

    /**
     * Stores a suspended coroutine waiting for the "write" permit
     * while upgrading from the "write-intent" one.
     */
    private var upgradingWriteIntent: CancellableContinuation<Unit>? = null

    /**
     * The number of active readers (acquired and not yet released "read" permits).
     */
    private var acquiredReadLocks = 0

    /**
     * Indicates whether there is an acquired and not yet released "write" permit.
     */
    private var isWriteLockAcquired = false

    /**
     * Indicates whether there is an acquired and not yet released (neither upgraded) "writ-intent" permit.
     */
    private var isWriteIntentLockAcquired = false

    suspend fun acquireReadPermit() {
        // Try to acquire a "read" permit without suspension.
        if (tryAcquireReadPermit()) return
        // Slow-path with suspension.
        acquireReadPermitSlowPath()
    }

    fun tryAcquireReadPermit(): Boolean = synchronizationLock.withLock {
        tryAcquireReadPermitInternal()
    }

    private suspend fun acquireReadPermitSlowPath() = suspendCancellableCoroutine<Unit> sc@{ cont ->
        synchronizationLock.withLock {
            // Try to acquire a "read" permit without suspension.
            if (tryAcquireReadPermitInternal()) {
                cont.resume(Unit) { releaseReadPermit() }
                return@sc
            }
            // Add this coroutine to the list of waiting readers.
            waitingReaders += cont
        }
        // On cancellation, remove this coroutine from the list.
        cont.invokeOnCancellation {
            synchronizationLock.withLock {
                waitingReaders.remove(cont)
            }
        }
    }

    /**
     * Tries to acquire a "read" permit without suspension.
     *
     * IMPLEMENTATION NOTE: this function must be called under [synchronizationLock].
     */
    private fun tryAcquireReadPermitInternal(): Boolean {
        // Is the write lock acquired?
        if (isWriteLockAcquired) return false
        // Is there a waiting "write" request?
        if (waitingWriters.isNotEmpty()) return false
        // Is there an upgrading "write-intent" waiting for the "write" permit?
        if (upgradingWriteIntent != null) return false
        // Acquire a "read" permit.
        acquiredReadLocks++
        return true
    }

    suspend fun acquireWritePermit() {
        // Try to acquire the "write" permit without suspension.
        if (tryAcquireWritePermit()) return
        // Slow-path with suspension.
        acquireWritePermitSlowPath()
    }

    fun tryAcquireWritePermit(): Boolean = synchronizationLock.withLock {
        tryAcquireWritePermitInternal()
    }

    private suspend fun acquireWritePermitSlowPath() = suspendCancellableCoroutine<Unit> sc@{ cont ->
        synchronizationLock.withLock {
            // Try to acquire the "write" permit without suspension.
            if (tryAcquireWritePermitInternal()) {
                cont.resume(Unit) { releaseWritePermit() }
                return@sc
            }
            // Add this coroutine to the list of waiting writers.
            waitingWriters += cont
        }
        // On cancellation, remove this coroutine from the list and
        // resume readers and one "write-intent" if this was the last waiting writer.
        cont.invokeOnCancellation {
            synchronizationLock.withLock {
                waitingWriters.remove(cont)
                if (!isWriteLockAcquired && waitingWriters.isEmpty() && upgradingWriteIntent == null) {
                    tryResumeReadersAndFirstWriteIntent()
                }
            }
        }
    }

    /**
     * Tries to acquire the "write" permit without suspension.
     *
     * IMPLEMENTATION NOTE: this function must be called under [synchronizationLock].
     */
    private fun tryAcquireWritePermitInternal(): Boolean {
        // Is there an active writer?
        if (isWriteLockAcquired) return false
        // Is there an active "write-intent"?
        if (isWriteIntentLockAcquired) return false
        // Is there an active reader?
        if (acquiredReadLocks > 0) return false
        // Acquire the "write" permit.
        isWriteLockAcquired = true
        return true
    }

    suspend fun acquireWriteIntentPermit() {
        // Try to acquire the "write-intent" permit without suspension.
        if (tryAcquireWriteIntentPermit()) return
        // Slow-path with suspension.
        acquireWriteIntentPermitSlowPath()
    }

    fun tryAcquireWriteIntentPermit(): Boolean = synchronizationLock.withLock {
        tryAcquireWriteIntentPermitInternal()
    }

    private suspend fun acquireWriteIntentPermitSlowPath() = suspendCancellableCoroutine<Unit> sc@{ cont ->
        synchronizationLock.withLock {
            // Try to acquire the "write" permit without suspension.
            if (tryAcquireWriteIntentPermitInternal()) {
                cont.resume(Unit) { releaseWriteIntentPermit() }
                return@sc
            }
            // Add this coroutine to the list of waiting writers.
            waitingWriteIntents += cont
        }
        // On cancellation, remove this coroutine from the list.
        cont.invokeOnCancellation {
            synchronizationLock.withLock {
                waitingWriteIntents.remove(cont)
            }
        }
    }

    /**
     * Tries to acquire the "write-intent" permit without suspension.
     *
     * IMPLEMENTATION NOTE: this function must be called under [synchronizationLock].
     */
    private fun tryAcquireWriteIntentPermitInternal(): Boolean {
        // Is the "write-intent" permit already acquired?
        if (isWriteIntentLockAcquired) return false
        // Is the write lock acquired?
        if (isWriteLockAcquired) return false
        // Is there a waiting "write" request?
        if (waitingWriters.isNotEmpty()) return false
        // Is there an upgrading "write-intent" waiting for the "write" permit?
        if (upgradingWriteIntent != null) return false
        // Acquire the write-intent permit.
        isWriteIntentLockAcquired = true
        return true
    }

    suspend fun upgradeWriteIntentToWrite() {
        // Try to upgrade the "write-intent" permit
        // to the "write" one without suspension.
        if (tryUpgradeWriteIntentToWrite()) return
        // Slow-path with suspension.
        upgradeWriteIntentToWriteSlowPath()
    }

    fun tryUpgradeWriteIntentToWrite(): Boolean = synchronizationLock.withLock {
        tryUpgradeWriteIntentToWriteInternal()
    }

    private suspend fun upgradeWriteIntentToWriteSlowPath() = suspendCancellableCoroutine<Unit> sc@{ cont ->
        synchronizationLock.withLock {
            // Try to upgrade the "write-intent" permit
            // to the "write" one without suspension.
            if (tryUpgradeWriteIntentToWriteInternal()) {
                cont.resume(Unit) { releaseWritePermit() }
                return@sc
            }
            // Suspend.
            isWriteIntentLockAcquired = true
            upgradingWriteIntent = cont
        }
        // On cancellation, remove this coroutine from the waiting list and
        // resume readers and one "write-intent" if this was the last waiting writer.
        cont.invokeOnCancellation {
            synchronizationLock.withLock {
                upgradingWriteIntent = null
                if (!isWriteLockAcquired && waitingWriters.isEmpty()) {
                    tryResumeReadersAndFirstWriteIntent()
                }
            }
        }
    }

    /**
     * Tries to upgrade the "write-intent" permit to the "write" one without suspension.
     *
     * IMPLEMENTATION NOTE: this function must be called under [synchronizationLock].
     */
    private fun tryUpgradeWriteIntentToWriteInternal(): Boolean {
        check(isWriteIntentLockAcquired) {
            "The 'write-intent' lock is not acquired"
        }
        // Is there an active reader?
        if (acquiredReadLocks > 0) return false
        // Upgrade the permit
        isWriteIntentLockAcquired = true
        isWriteLockAcquired = true
        return true
    }

    fun releaseReadPermit(): Unit = synchronizationLock.withLock {
        check(acquiredReadLocks > 0) {
            "No 'read' lock is acquired"
        }
        acquiredReadLocks--
        // Is it the last reader?
        if (acquiredReadLocks == 0 && (!isWriteIntentLockAcquired || upgradingWriteIntent != null)) {
            tryResumeFirstWriter()
        }
    }

    fun releaseWritePermit(): Unit = synchronizationLock.withLock {
        check(isWriteLockAcquired) {
            "The 'write' lock is not acquired"
        }
        isWriteLockAcquired = false
        // Try resume first writer, resuming readers only
        // if no writer is waiting for the permit.
        if (isWriteIntentLockAcquired || !tryResumeFirstWriter()) {
            tryResumeReadersAndFirstWriteIntent()
        }
    }

    fun releaseWriteIntentPermit(): Unit = synchronizationLock.withLock {
        check(isWriteIntentLockAcquired) {
            "The 'write-intent' lock is not acquired"
        }
        isWriteIntentLockAcquired = false
        // Is it the last reader?
        if (acquiredReadLocks == 0) {
            if (tryResumeFirstWriter()) return
        }
        // Is there another waiting "write-intent"?
        while (waitingWriteIntents.isNotEmpty()) {
            val w = waitingWriteIntents.removeFirst()
            val resumed = w.tryResumeInternal(Unit) { releaseWriteIntentPermit() }
            if (resumed) {
                isWriteIntentLockAcquired = true
                break
            }
        }
    }

    /**
     * Tries to resume all suspended "read" and the first "write-intent" operations.
     *
     * IMPLEMENTATION NOTE: this function must be called under [synchronizationLock].
     */
    private fun tryResumeReadersAndFirstWriteIntent() {
        // Resume all waiting readers.
        waitingReaders.forEach {
            val resumed = it.tryResumeInternal(Unit) { releaseReadPermit() }
            if (resumed) {
                acquiredReadLocks++
            }
        }
        // Resume the first "write-intent" operation if there is no active one.
        if (!isWriteIntentLockAcquired) {
            while (waitingWriteIntents.isNotEmpty()) {
                val w = waitingWriteIntents.removeFirst()
                val resumed = w.tryResumeInternal(Unit) { releaseWriteIntentPermit() }
                if (resumed) {
                    isWriteIntentLockAcquired = true
                    break
                }
            }
        }
    }

    /**
     * Tries to resume the first suspended writer,
     * prioritizing upgrading "write-intent" operations.
     *
     * IMPLEMENTATION NOTE: this function must be called under [synchronizationLock].
     *
     * @return `true` if a writer has been successfully resumed,
     * and `false` otherwise.
     */
    private fun tryResumeFirstWriter(): Boolean {
        // Is there upgrading "write-intent" request?
        upgradingWriteIntent?.let {
            // Clean the reference.
            upgradingWriteIntent = null
            // Try to resume the operation.
            val resumed = it.tryResumeInternal(Unit) { releaseWritePermit() }
            if (resumed) {
                isWriteLockAcquired = true
                return true
            }
        }
        // Try to resume the first writer.
        while (waitingWriters.isNotEmpty()) {
            val w = waitingWriters.removeFirst()
            val resumed = w.tryResumeInternal(Unit) { releaseWritePermit() }
            if (resumed) {
                isWriteLockAcquired = true
                return true
            }
        }
        // No waiting writers.
        return false
    }
}

@OptIn(InternalCoroutinesApi::class)
private fun <T> CancellableContinuation<T>.tryResumeInternal(value: T, onCancellation: (Throwable?) -> Unit): Boolean {
    tryResume(value, null, onCancellation).let {
        if (it == null) return false
        completeResume(it)
        return true
    }
}