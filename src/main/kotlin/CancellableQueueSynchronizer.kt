/*
 * Copyright 2016-2023 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER", "CANNOT_OVERRIDE_INVISIBLE_MEMBER")
@file:OptIn(
    ExperimentalCoroutinesApi::class, InternalCoroutinesApi::class
)

package rwmutex

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.selects.*
import kotlinx.coroutines.sync.*
import rwmutex.CancellableQueueSynchronizer.*
import rwmutex.CancellableQueueSynchronizer.CancellationMode.*
import rwmutex.CancellableQueueSynchronizer.ResumeMode.*
import kotlin.coroutines.*
import kotlin.math.*

/**
[CancellableQueueSynchronizer] (CQS) is an abstraction for implementing _fair_ synchronization and communication primitives.
Essentially, It maintains a FIFO queue of waiting requests and provides two main functions:
- [suspend] that stores the specified waiter into the queue, and
- [resume] that tries to retrieve and resume the first waiter, passing the specified value to it.
The key advantage of these semantics is that CQS allows to invoke [resume] before [suspend] as long as
it is known that [suspend] will happen eventually. For example, our [Semaphore] implementation actively
uses this property for better performance.

A useful mental image of [CancellableQueueSynchronizer] is that of an infinite array with two positioning counters:
one references the next cell in which a new waiter is enqueued as a part of the next [suspend] call,
while another references the next cell for [resume]. The intuition is that [suspend] atomically increments
its counter via `Fetch-and-Add` and stores the waiter in the corresponding cell. Likewise, [resume] increments
its counter, visits the corresponding cell, and resumes the stored waiter with the specified value.

_Synchronous and Asynchronous Resumption Modes_

Notably, [resume] may come to the cell before [suspend] and find the cell in the empty state.
To solve this race, we introduce two [resumption modes][ResumeMode]: [synchronous][SYNC] and [asynchronous][ASYNC].
In both case, [resume] puts the value into the empty cell, and then either finishes immediately
in the [asynchronous][ASYNC] mode, or waits until the value is taken by a concurrent [suspend]
in the [synchronous][SYNC] one. In the latter case, if the value is not taken within a bounded time, [resume] marks
the cell as _broken_. Thus, both this [resume] and the corresponding [suspend] fail. The intuition is that allowing
for broken cells keeps the balance of pairwise operations, such as [acquire()][Semaphore.acquire]
and [release()][Semaphore.release] in [Semaphore], so these operations simply restart in case of breaking the cell.
This way, we can achieve wait-freedom with the [asynchronous][ASYNC] mode, and obstruction-freedom
with the [synchronous][SYNC] mode.

_Cancellation Support_

We support two cancellation policies in [CancellableQueueSynchronizer]. In the [simple cancellation mode][SIMPLE],
[resume] fails and returns `false` if it finds the cell in the `CANCELLED` state or if the waiter resumption
(see [CancellableContinuation.tryResume]) fails. These failures are typically handled by restarting the operation
from the beginning. With the [smart cancellation][SMART], [resume] efficiently skips `CANCELLED` cells
(the cells where waiter resumption failed are also considered as `CANCELLED`). This way, even if a million of
canceled requests are stored in [CancellableQueueSynchronizer], one [resume] invocation is sufficient to pass
the value to the next alive waiter since it skips all these canceled waiters.  However, the smart cancellation mode
provides less intuitive contract and requires users to write more complicated code -- the details are discussed further.

The main issue with skipping `CANCELLED` cells in [resume] is that it can become illegal to put the value into
the next cell. Consider the following execution: [suspend] is called, then [resume] starts, but the suspended
waiter becomes canceled. This way, no one is waiting in [CancellableQueueSynchronizer] anymore. Thus, if [resume]
skips this canceled cell, puts the value into the next empty cell, and completes, the data structure's state becomes
incorrect. Instead, the value provided by this [resume] should be refused and returned to the outer data structure.
Unfortunately, there is no way for [CancellableQueueSynchronizer] to decide whether the value should be refused or not.
Thus, users should implement a custom cancellation handler by overriding the [onCancellation] function, which must
return `true` if the cancellation completes successfully and `false` if the [resume] that will come to this cell
should be refused. In the latter case, the [resume] that comes to this cell invokes [tryReturnRefusedValue] to return
the value back to the outer data structure. However, it is possible for [tryReturnRefusedValue] to fail, and
[returnValue] is called in this case. Typically, this [returnValue] function coincides with the one that resumes waiters
(e.g., with [release][Semaphore.release] in [Semaphore]). There is also an important difference between [synchronous][SYNC]
and [asynchronous][ASYNC] resumption modes. In the [synchronous][SYNC] mode, the [resume] that comes to a cell with
a canceled waiter (but the cell is not in the `CANCELLED` state yet) waits in a spin-loop until the cancellation handler
is processed and the cell is moved to either `CANCELLED` or `REFUSE` state. In contrast, in the [asynchronous][ASYNC] mode,
[resume] replaces the canceled waiter with the value of this resumption and finishes immediately -- the cancellation handler
completes this [resume] eventually. This way, in the [asynchronous][ASYNC] mode, the value passed to [resume] can be out
of the data structure for a while but is guaranteed to be processed eventually.

To support prompt cancellation, [CancellableQueueSynchronizer] returns the value back to the data structure by calling
[returnValue] if the continuation is cancelled while dispatching. Typically, [returnValue] delegates to the operation
that calls [resume], such as [release][Semaphore.release] in [Semaphore].

_Algorithm Details_

Please see the ["CQS: A Formally-Verified Framework for Fair and Abortable Synchronization"](TODO)
paper by Nikita Koval, Dmitry Kaplansky, and Dan Alistarh for the detailed algorithm description.
 */
internal abstract class CancellableQueueSynchronizer<T : Any> {
    /*
     The counters indicate the total numbers of `suspend` and `resume` calls ever performed.
     They are incremented in the beginning of the corresponding operation;
     thus, acquiring a unique (for the operation type) cell to process.
     The segments reference the last working one for each operation type.
    */
    private val suspendIdx = atomic(0L)
    private val suspendSegment: AtomicRef<CQSSegment>
    private val resumeIdx = atomic(0L)
    private val resumeSegment: AtomicRef<CQSSegment>

    init {
        val s = CQSSegment(id = 0, prev = null, pointers = 2)
        resumeSegment = atomic(s)
        suspendSegment = atomic(s)
    }

    /**
     * Specifies whether [resume] should work in
     * [synchronous][SYNC] or [asynchronous][ASYNC] mode.
     */
    protected open val resumeMode: ResumeMode get() = SYNC

    /**
     * Specifies whether [resume] should fail on cancelled waiters ([SIMPLE] mode) or
     * skip them ([SMART] mode). Remember that in case of [smart][SMART] cancellation mode,
     * the [onCancellation] handler should be implemented.
     */
    protected open val cancellationMode: CancellationMode get() = SIMPLE

    /**
     * Specifies whether cancelled suspended coroutines should wait for the refused value to be returned.
     */
    protected open val waitUntilProcessed: Boolean get() = false

    /**
     * This function is called when waiter is cancelled and smart
     * cancellation mode is used (so cancelled cells are skipped by
     * [resume]). By design, this handler performs the logical cancellation
     * and returns `true` if the cancellation succeeds and the cell can be
     * moved to the `CANCELLED` state. In this case, [resume] skips the cell and passes
     * the value to the next waiter in the waiting queue. However, if the [resume]
     * that comes to this cell should be refused, [onCancellation] should return false.
     * In this case, [tryReturnRefusedValue] is invoked with the value of this [resume],
     * following by [returnValue] if [tryReturnRefusedValue] fails.
     */
    protected open fun onCancellation(): Boolean = error("not implemented")

    /**
     * This function specifies how the value refused by this [CancellableQueueSynchronizer]
     * (when [onCancellation] returns `false`) should be transferred back to the data structure.
     * It returns `true` on success and `false` when the attempt fails. In the latter case,
     * [returnValue] is used to complete the returning process.
     */
    protected open fun tryReturnRefusedValue(value: T): Boolean = true

    /**
     * This function specifies how the value from a failed [resume] should be returned back to
     * the data structure. Typically, this function delegates to the one that invokes [resume]
     * (e.g., [release()][Semaphore.release] in [Semaphore]).
     *
     * This function is invoked when [onCancellation] returns `false` and the following [tryReturnRefusedValue]
     * fails, or when prompt cancellation occurs and the value should be returned back to the data structure.
     * TODO: we need to merge the PR that optimizes this code
     */
    protected open fun returnValue(value: T) {}

    /**
     * This is a shortcut for [tryReturnRefusedValue] and
     * the following [returnValue] invocation on failure.
     */
    private fun returnRefusedValue(value: T) {
        if (tryReturnRefusedValue(value)) return
        returnValue(value)
    }

    internal fun suspendCancelled(): T? {
        // Increment `suspendIdx` and find the segment
        // with the corresponding id. It is guaranteed
        // that this segment is not removed since at
        // least the cell for this `suspend` invocation
        // is not in the `CANCELLED` state.
        val curSuspendSegm = this.suspendSegment.value
        val suspendIdx = suspendIdx.getAndIncrement()

        val segment = this.suspendSegment.findSegmentAndMoveForward(
            id = suspendIdx / SEGMENT_SIZE, startFrom = curSuspendSegm,
            createNewSegment = ::createSegment
        ).segment
        // Try to install the waiter into the cell - this is the regular path.
        val i = (suspendIdx % SEGMENT_SIZE).toInt()
        if (segment.cas(i, null, CANCELLED)) {
            // The continuation is successfully installed, and
            // `resume` cannot break the cell now, so this
            // suspension is successful.
            // Add a cancellation handler if required and finish.
            return null
        }
        // The continuation installation has failed. This happened because a concurrent
        // `resume` came earlier to this cell and put its value into it. Remember that
        // in the `SYNC` resumption mode this concurrent `resume` can mark the cell as broken.
        //
        // Try to grab the value if the cell is not in the `BROKEN` state.
        val value = segment.get(i)
        if (value !== BROKEN && segment.cas(i, value, TAKEN)) {
            // The elimination is performed successfully,
            // complete with the value stored in the cell.
            @Suppress("UNCHECKED_CAST")
            return value as T
        }
        // The cell is broken, this can happen only in the `SYNC` resumption mode.
        assert { resumeMode == SYNC && segment.get(i) === BROKEN }
        return null
    }

    @Suppress("UNCHECKED_CAST")
    @OptIn(InternalCoroutinesApi::class)
    internal fun suspend(waiter: Waiter): Boolean {
        // Increment `suspendIdx` and find the segment
        // with the corresponding id. It is guaranteed
        // that this segment is not removed since at
        // least the cell for this `suspend` invocation
        // is not in the `CANCELLED` state.
        val curSuspendSegm = this.suspendSegment.value
        val suspendIdx = suspendIdx.getAndIncrement()

        val segment = this.suspendSegment.findSegmentAndMoveForward(
            id = suspendIdx / SEGMENT_SIZE, startFrom = curSuspendSegm,
            createNewSegment = ::createSegment
        ).segment
        assert { segment.id == suspendIdx / SEGMENT_SIZE }
        // Try to install the waiter into the cell - this is the regular path.
        val i = (suspendIdx % SEGMENT_SIZE).toInt()
        if (segment.cas(i, null, waiter)) {
            // The continuation is successfully installed, and
            // `resume` cannot break the cell now, so this
            // suspension is successful.
            // Add a cancellation handler if required and finish.
            waiter.invokeOnCancellation(segment, i)
            return true
        }
        // The continuation installation has failed. This happened because a concurrent
        // `resume` came earlier to this cell and put its value into it. Remember that
        // in the `SYNC` resumption mode this concurrent `resume` can mark the cell as broken.
        //
        // Try to grab the value if the cell is not in the `BROKEN` state.
        val value = segment.get(i)
        if (value !== BROKEN && segment.cas(i, value, TAKEN)) {
            // The elimination is performed successfully,
            // complete with the value stored in the cell.
            value as T
            when (waiter) {
                is CancellableContinuation<*> -> {
                    waiter as CancellableContinuation<T>
                    waiter.resume(value, { returnValue(value) }) // TODO do we really need this?
                }

                is SelectInstance<*> -> {
                    waiter as SelectInstance<T>
                    waiter.selectInRegistrationPhase(value)
                }
            }
            return true
        }
        // The cell is broken, this can happen only in the `SYNC` resumption mode.
        assert { resumeMode == SYNC && segment.get(i) === BROKEN }
        return false
    }

    /**
     * Tries to resume the next waiter and returns `true` if
     * the resumption succeeds. However, it can fail due to
     * several reasons. First, if the [synchronous][SYNC] resumption
     * mode is used, this [resume] invocation may come before [suspend],
     * find the cell in the empty state, mark it as [broken][BROKEN],
     * and fail returning `false` as a result. Another reason for [resume]
     * to fail is waiter cancellation if the [simple cancellation mode][SIMPLE]
     * is used.
     *
     * Note that with the [smart][SMART] cancellation mode [resume] skips
     * cancelled waiters and can fail only in case of unsuccessful elimination
     * due to [synchronous][SYNC] resumption.
     */
    fun resume(value: T): Boolean {
        // Should we skip cancelled cells?
        val skipCancelled = cancellationMode != SIMPLE
        while (true) {
            // Try to resume the next waiter, adjust `resumeIdx` if
            // cancelled cells will be skipped anyway.
            when (tryResumeImpl(value = value, adjustResumeIdx = skipCancelled)) {
                TRY_RESUME_SUCCESS -> return true
                TRY_RESUME_FAIL_CANCELLED -> if (!skipCancelled) return false
                TRY_RESUME_FAIL_BROKEN -> return false
            }
        }
    }

    /**
     * Tries to resume the next waiter, and returns [TRY_RESUME_SUCCESS] on
     * success, [TRY_RESUME_FAIL_CANCELLED] if the next waiter is cancelled,
     * or [TRY_RESUME_FAIL_BROKEN] if the next cell has been marked as broken
     * by this [tryResumeImpl] invocation due to a race in the [SYNC] resumption mode.
     *
     * In the [smart cancellation mode][SMART], all cells marked as
     * [cancelled][CANCELLED] should be skipped, so there is no need
     * to increment [resumeIdx] one-by-one if there is a removed segment
     * (logically full of [cancelled][CANCELLED] cells). Instead, the algorithm
     * moves [resumeIdx] to the first possibly non-cancelled cell, i.e.,
     * to the first segment id multiplied by [SEGMENT_SIZE].
     */
    private fun tryResumeImpl(value: T, adjustResumeIdx: Boolean): Int {
        // Check that `adjustResumeIdx` is `false` in the simple cancellation mode.
        check(!(cancellationMode == SIMPLE && adjustResumeIdx))
        // Increment `resumeIdx` and find the first segment with
        // the corresponding or higher (if the required segment
        // is physically removed) id.
        val curResumeSegm = this.resumeSegment.value
        val resumeIdx = resumeIdx.getAndIncrement()
        val id = resumeIdx / SEGMENT_SIZE
        val segment = this.resumeSegment.findSegmentAndMoveForward(
            id, startFrom = curResumeSegm,
            createNewSegment = ::createSegment
        ).segment
        // The previous segments can be safely collected by GC, clean the pointer to them.
        segment.cleanPrev()
        // Is the required segment physically removed?
        if (segment.id > id) {
            // Adjust `resumeIdx` to the first non-removed segment if needed.
            if (adjustResumeIdx) adjustResumeIdx(segment.id * SEGMENT_SIZE)
            // The cell #resumeIdx is in the `CANCELLED` state,  return the corresponding failure.
            return TRY_RESUME_FAIL_CANCELLED
        }
        // Modify the cell according to the state machine,
        // all the transitions are performed atomically.
        val i = (resumeIdx % SEGMENT_SIZE).toInt()
        modify_cell@ while (true) {
            val cellState = segment.get(i)
            when {
                // Is the cell empty?
                cellState === null -> {
                    // Try to perform an elimination by putting the
                    // value to the empty cell and wait until it is
                    // taken by a concurrent `suspend` in case of
                    // using the synchronous resumption mode.
                    if (!segment.cas(i, null, value)) continue@modify_cell
                    // Finish immediately in the asynchronous resumption mode.
                    if (resumeMode == ASYNC) return TRY_RESUME_SUCCESS
                    // Wait for a concurrent `suspend` (which should mark
                    // the cell as taken) for a bounded time in a spin-loop.
                    //var iteration = 0
                    while (true) {
                        if (segment.get(i) === TAKEN) return TRY_RESUME_SUCCESS
                        //iteration++
                        //if (resumeMode == SYNC && iteration > MAX_SPIN_CYCLES) break
                    }
                    // The value is still not taken, try to atomically mark the cell as broken.
                    // A CAS failure indicates that the value is successfully taken.
                    //return if (segment.cas(i, value, BROKEN)) TRY_RESUME_FAIL_BROKEN else TRY_RESUME_SUCCESS
                }
                // Is the waiter cancelled?
                cellState === CANCELLED -> {
                    // Return the corresponding failure.
                    return TRY_RESUME_FAIL_CANCELLED
                }
                // Should the current `resume` be refused by this CQS?
                cellState === REFUSE -> {
                    // This state should not occur
                    // in the simple cancellation mode.
                    assert { cancellationMode != SIMPLE }
                    // Return the refused value back to the
                    // data structure and finish successfully.
                    returnRefusedValue(value)
                    if (waitUntilProcessed)
                        segment.set(i, PROCESSED)
                    return TRY_RESUME_SUCCESS
                }
                // Does the cell store a cancellable continuation?
                cellState is Waiter -> {
                    // Change the cell state to `RESUMED`, so
                    // the cancellation handler cannot be invoked
                    // even if the continuation becomes cancelled.
                    if (!segment.cas(i, cellState, RESUMED)) continue@modify_cell
                    // Try to resume the continuation.
                    val resumed = when (cellState) {
                        is CancellableContinuation<*> -> {
                            (cellState as CancellableContinuation<T>)
                            val token = cellState.tryResume(value, null) { returnValue(value) }
                            if (token != null) {
                                // Hooray, the continuation is successfully resumed!
                                cellState.completeResume(token)
                                true
                            } else {
                                false
                            }
                        }

                        is SelectInstance<*> -> {
                            cellState.trySelect(this@CancellableQueueSynchronizer, value)
                        }

                        else -> error("unexpected")
                    }
                    if (!resumed) {
                        // Unfortunately, the continuation resumption has failed.
                        // Fail the current `resume` if the simple cancellation mode is used.
                        if (cancellationMode === SIMPLE)
                            return TRY_RESUME_FAIL_CANCELLED
                        // In the smart cancellation mode, the cancellation handler should be invoked.
                        val cancelled = onCancellation()
                        if (cancelled) {
                            // We could mark the cell as `CANCELLED` for consistency,
                            // but there is no need for this since the cell cannot
                            // be processed by another operation anymore.
                            //
                            // Try to resume the next waiter. If the resumption fails due to
                            // a race in the synchronous mode, the value should be returned
                            // back to the data structure.
                            if (!resume(value)) returnValue(value)
                        } else {
                            // The value is refused by this CQS, return it back to the data structure.
                            returnRefusedValue(value)
                        }
                    }
                    // Once the state is changed to `RESUMED`, `resume` is considered as successful.
                    if (waitUntilProcessed)
                        segment.set(i, PROCESSED)
                    return TRY_RESUME_SUCCESS
                }
                // Does the cell store a cancelling waiter, which is already logically
                // cancelled but the cancellation handler has not been completed yet?
                //cellState === CANCELLING -> {
                cellState is Thread -> {
                    // Fail in the simple cancellation mode.
                    if (cancellationMode == SIMPLE) return TRY_RESUME_FAIL_CANCELLED
                    // In the smart cancellation mode, this cell should be either skipped
                    // (when it becomes `CANCELLED`), or the current `resume` should be refused.
                    //
                    // In the synchronous resumption mode, `resume(..)` waits in a an unbounded spin-loop until
                    // the state of this cell is changed to either `CANCELLED` or `REFUSE`. While this part makes
                    // the overall algorithm blocking in theory, this cancellation handler and `resume` overlap occurs
                    // relatively rare in practice and it is guaranteed that one cancellation can block at most one
                    // `resume`, what makes the algorithm almost non-blocking in any real-world high-contended scenario.
                    if (resumeMode == SYNC && cellState !== Thread.currentThread()) continue@modify_cell
                    // In the asynchronous resumption mode, `resume` puts the resumption value into the cell,
                    // so the concurrent cancellation handler completes this `resume` after it decides whether
                    // the cell should be marked as `CANCELLED` or `REFUSE`. Thus, this `resume` is delegated to
                    // the cancellation handler and can be postponed for a while.
                    //
                    // To distinguish continuations related to the `suspend` operation with the continuations passed
                    // as values (this is strange but possible), we wrap the last ones with `WrappedContinuationValue`.
                    val valueToStore: Any = if (value is Continuation<*>) WrappedContinuationValue(value) else value
                    if (segment.cas(i, cellState, valueToStore)) return TRY_RESUME_SUCCESS
                }
                // The cell stores a plane non-cancellable continuation, we can simply resume it.
                cellState is Continuation<*> -> {
                    // Resume the continuation and mark the cell
                    // as `RESUMED` to avoid memory leaks.
                    segment.set(i, RESUMED)
                    @Suppress("UNCHECKED_CAST")
                    (cellState as Continuation<T>).resume(value)
                    return TRY_RESUME_SUCCESS
                }

                else -> error("Unexpected cell state: $cellState")
            }
        }
    }

    /**
     * Updates [resumeIdx] to [newValue] if the current value is lower.
     */
    private fun adjustResumeIdx(newValue: Long): Unit = resumeIdx.loop { cur ->
        if (cur >= newValue) return
        if (resumeIdx.compareAndSet(cur, newValue)) return
    }

    /**
     * These modes define the strategy that [resume] should
     * use if it comes to the cell before [suspend] and finds it empty.
     * In the [asynchronous][ASYNC] mode, [resume] puts the value into the cell,
     * so [suspend] grabs it after that and completes without actual suspension.
     * In other words, an elimination happens in this case.
     *
     * However, such a strategy produces an incorrect behavior when used for some
     * data structures (e.g., for [tryAcquire][Semaphore.tryAcquire] in [Semaphore]),
     * so the [synchronous][SYNC] mode has been introduced in addition.
     * Similarly to the asynchronous one, [resume] puts the value into the cell,
     * but do not finish immediately. In opposite, it waits in a bounded spin-loop
     * (see [MAX_SPIN_CYCLES]) until the value is taken and completes only after that.
     * If the value is not taken after this spin-loop ends, [resume] marks the cell as
     * [broken][BROKEN] and fails, so the corresponding [suspend] invocation finds the cell
     * [broken][BROKEN] and fails as well.
     */
    internal enum class ResumeMode { SYNC, SYNC_BLOCKING, ASYNC }

    /**
     * These modes define the strategy that should be used when a waiter becomes cancelled.
     *
     * In the [simple cancellation mode][SIMPLE], [resume] fails when the waiter in the working cell is cancelled.
     * In the [smart cancellation mode][SMART], [resume] skips cancelled cells and passes the value to the first
     * non-cancelled waiter. However, it is also possible that the cancelled waiter was the last one, so this
     * [resume] should be refused (in this case, the corresponding [onCancellation] call returns false), and
     * the value is returned back to the data structure via [returnValue].
     */
    internal enum class CancellationMode { SIMPLE, SMART }

    private fun createSegment(id: Long, prev: CQSSegment?) = CQSSegment(id, prev, 0)

    /**
     * The queue of waiters in [CancellableQueueSynchronizer] is represented as a linked list of [CQSSegment].
     */
    internal inner class CQSSegment(id: Long, prev: CQSSegment?, pointers: Int) :
        Segment<CQSSegment>(id, prev, pointers) {

        private val waiters = atomicArrayOfNulls<Any?>(SEGMENT_SIZE)
        override val numberOfSlots get() = SEGMENT_SIZE

        override fun onCancellation(index: Int, cause: Throwable?, context: CoroutineContext) {
            // Invoke the cancellation handler
            // only if the state is not `RESUMED`.
            //
            // After the state is changed to `RESUMED`, the
            // resumption is considered as logically successful,
            // and the value can be returned back to the data structure
            // only via a `returnValue(..)` call.
            if (!tryMarkCancelling(index)) {
                if (waitUntilProcessed)
                    while (get(index) !== PROCESSED) {
                    }
                return
            }
            // Do we use simple or smart cancellation?
            if (cancellationMode === SIMPLE) {
                // In the simple cancellation mode the logic
                // is straightforward -- mark the cell as
                // cancelled to avoid memory leaks and complete.
                markCancelled(index)
                return
            }
            // We are in the smart cancellation mode.
            // Invoke `onCancellation()` and mark the cell as `CANCELLED`
            // if the call returns `true`, or as `REFUSE` if it
            // returns `false`. Note that it is possible for a
            // concurrent `resume(..)` to put its resumption value
            // into the cell in the asynchronous mode. In this case,
            // the cancellation handler should complete this `resume(..)`.
            val cancelled = onCancellation()
            if (cancelled) {
                // The cell should be considered as cancelled.
                // Mark the cell correspondingly and help a concurrent
                // `resume(..)` to process its value if needed.
                val value = markCancelled(index) ?: return
                // Resume the next waiter with the value
                // provided by a concurrent `resume(..)`.
                // The value could be put only in the asynchronous mode,
                // so the `resume(..)` call above must not fail.
                @Suppress("UNCHECKED_CAST")
                resume(value as T)
            } else {
                // The `resume(..)` that will come to this cell should be refused.
                // Mark the cell correspondingly and help a concurrent
                // `resume(..)` to process its value if needed.
                val value = markRefuse(index)
                if (value === null) {
                    if (waitUntilProcessed)
                        while (get(index) !== PROCESSED) {
                        }
                    return
                }
                @Suppress("UNCHECKED_CAST")
                returnRefusedValue(value as T)
            }
        }

        @Suppress("NOTHING_TO_INLINE")
        inline fun get(index: Int): Any? = waiters[index].value

        @Suppress("NOTHING_TO_INLINE")
        inline fun set(index: Int, value: Any?) {
            waiters[index].value = value
        }

        @Suppress("NOTHING_TO_INLINE")
        inline fun cas(index: Int, expected: Any?, value: Any?): Boolean = waiters[index].compareAndSet(expected, value)

        @Suppress("NOTHING_TO_INLINE")
        inline fun getAndSet(index: Int, value: Any?): Any? = waiters[index].getAndSet(value)

        /**
         * In the CQS algorithm, we use different handlers for normal and prompt cancellations.
         * However, the current [CancellableContinuation] API (which is, hopefully, subject to change)
         * does not allow to split the handlers -- the one set by [CancellableContinuation.invokeOnCancellation]
         * is always invoked, even when prompt cancellation occurs. To guarantee that only the proper handler is used
         * (either the one installed by `invokeOnCancellation { ... }` or the one passed to `tryResume(..)`),
         * we use a special intermediate `CANCELLING` state for the normal cancellation. Thus, once the waiter becomes
         * cancelled, it should be atomically replaced with the `CANCELLING` marker. At the same time, if the state
         * is already `RESUMED`, the continuation is considered as logically resumed, [tryMarkCancelling] returns false,
         * and any cancellation is considered as prompt one, so the handler passed to `tryResume(..)` is used.
         * This handler simply returns the value back to the data structure via `returnValue(..)` invocation.
         */
        fun tryMarkCancelling(index: Int): Boolean {
            while (true) {
                val cellState = get(index)
                when {
                    cellState === RESUMED -> return false
                    cellState === PROCESSED -> return false
                    cellState is Waiter -> {
                        //if (cas(index, cellState, CANCELLING)) return true
                        if (cas(index, cellState, Thread.currentThread())) return true
                    }

                    else -> {
                        if (cellState is Continuation<*>)
                            error("Only cancellable continuations can be cancelled, ${cellState::class.simpleName} has been detected")
                        else
                            error("Unexpected cell state: $cellState")
                    }
                }
            }
        }

        /**
         * Atomically replaces [CANCELLING] with [CANCELLED] and returns `null` on success.
         * However, in the asynchronous resumption mode, [resume] may to come to the cell
         * while it is in the [CANCELLING] state, replace the [CANCELLING] marker with the
         * resumption value, and finish, delegating the rest of the resumption. In this case,
         * the function returns this value, and the caller must complete the resumption.
         *
         * In addition, this function checks whether the segment becomes full of cancelled
         * cells, and physically removes the segment from the linked list in this case; thus,
         * avoiding possible memory leaks caused by cancellation.
         */
        fun markCancelled(index: Int): Any? = mark(index, CANCELLED).also {
            onSlotCleaned()
        }

        /**
         * Atomically replaces [CANCELLING] with [REFUSE] and returns `null` on success.
         * However, in the asynchronous resumption mode, [resume] may to come to the cell
         * while it is in the [CANCELLING] state, replace the [CANCELLING] marker with the
         * resumption value, and finish, delegating the rest of the resumption. In this case,
         * the function returns this value, and the caller must complete the resumption.
         */
        fun markRefuse(index: Int): Any? = mark(index, REFUSE)

        /**
         * Updates the cell state to either [CANCELLED] or [REFUSE] from [CANCELLING], the
         * corresponding update [marker] is passed as an argument. However, in the asynchronous
         * resumption mode, it is possible for [resume] to come to the cell while it is in
         * the [CANCELLING] state. In this case, [resume] replaces the [CANCELLING] marker
         * with the resumption value and finishes, delegating the rest of the resumption to
         * the concurrent cancellation handler. Therefore, this [mark] function atomically
         * checks whether there is a value put into the cell by a concurrent [resume] and
         * either returns this value if found, or `null` if the cell was in the [CANCELLING] state.
         */
        private fun mark(index: Int, marker: Any?): Any? {
            val old = getAndSet(index, marker)
            // The cell should be in the `CANCELLING` state or store
            // an asynchronously put value at the point of this update.
            assert { old !== RESUMED && old !== CANCELLED && old !== REFUSE && old !== TAKEN && old !== BROKEN }
            assert { old !is Continuation<*> }
            // Return `null` if no value has been passed in meantime.
            //if (old === CANCELLING) return null
            if (old is Thread) return null
            // A concurrent `resume(..)` has put a value into the cell, return it as a result.
            return if (old is WrappedContinuationValue) old.cont else old
        }

        override fun toString() = "CQSSegment[id=$id, hashCode=${hashCode()}]"
    }

    // We use this string representation for traces in Lincheck tests
    override fun toString(): String {
        val waiters = ArrayList<String>()
        var curSegment = resumeSegment.value
        var curIdx = resumeIdx.value
        while (curIdx < max(suspendIdx.value, resumeIdx.value)) {
            val i = (curIdx % SEGMENT_SIZE).toInt()
            waiters += when {
                curIdx < curSegment.id * SEGMENT_SIZE -> "CANCELLED"
                curSegment.get(i) is Continuation<*> -> "<cont>"
                else -> curSegment.get(i).toString()
            }
            curIdx++
            if (curIdx == (curSegment.id + 1) * SEGMENT_SIZE)
                curSegment = curSegment.next ?: break
        }
        return "suspendIdx=${suspendIdx.value},resumeIdx=${resumeIdx.value},waiters=$waiters"
    }
}

/**
 * In the [smart cancellation mode][CancellableQueueSynchronizer.CancellationMode.SMART]
 * it is possible for [resume] to come to a cell with cancelled continuation and
 * asynchronously put the resumption value into the cell, so the cancellation handler decides whether
 * this value should be used for resuming the next waiter or be refused. When this
 * value is a continuation, it is hard to distinguish it with the one related to the cancelled
 * waiter. To solve the problem, such values of type [Continuation] are wrapped with
 * [WrappedContinuationValue]. Note that the wrapper is required only in [CancellableQueueSynchronizer.CancellationMode.SMART]
 * mode and is used in the asynchronous race resolution logic between cancellation and [resume]
 * invocation; this way, it is used relatively rare.
 */
private class WrappedContinuationValue(val cont: Continuation<*>)

private val SEGMENT_SIZE = systemProp("kotlinx.coroutines.cqs.segmentSize", 2)
private val MAX_SPIN_CYCLES = systemProp("kotlinx.coroutines.cqs.maxSpinCycles", 2)
private val TAKEN = Symbol("TAKEN")
private val BROKEN = Symbol("BROKEN")
private val CANCELLING = Symbol("CANCELLING")
private val CANCELLED = Symbol("CANCELLED")
private val REFUSE = Symbol("REFUSE")
private val PROCESSED = Symbol("PROCESSED")
private val RESUMED = Symbol("RESUMED")

private const val TRY_RESUME_SUCCESS = 0
private const val TRY_RESUME_FAIL_CANCELLED = 1
private const val TRY_RESUME_FAIL_BROKEN = 2