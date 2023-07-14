@file:Suppress("INVISIBLE_REFERENCE", "INVISIBLE_MEMBER", "CANNOT_OVERRIDE_INVISIBLE_MEMBER")

package rwmutex

import kotlinx.atomicfu.*
import kotlinx.coroutines.internal.*

/**
 * Returns the first segment `s` with `s.id >= id` or `CLOSED`
 * if all the segments in this linked list have lower `id`, and the list is closed for further segment additions.
 */
private fun <S : Segment<S>> S.findSegmentInternal(
    id: Long,
    createNewSegment: (id: Long, prev: S) -> S
): SegmentOrClosed<S> {
    /*
       Go through `next` references and add new segments if needed, similarly to the `push` in the Michael-Scott
       queue algorithm. The only difference is that "CAS failure" means that the required segment has already been
       added, so the algorithm just uses it. This way, only one segment with each id can be added.
     */
    var cur: S = this
    while (cur.id < id || cur.isRemoved) {
        val next = cur.next
        if (next !== null) { // there is a next node -- move there
            cur = next
            continue
        }
        val newTail = createNewSegment(cur.id + 1, cur)
        if (cur.trySetNext(newTail)) { // successfully added new node -- move there
            if (cur.isRemoved) cur.remove()
            cur = newTail
        }
    }
    return SegmentOrClosed(cur)
}

/**
 * Returns `false` if the segment `to` is logically removed, `true` on a successful update.
 */
@Suppress("NOTHING_TO_INLINE") // Must be inline because it is an AtomicRef extension
internal inline fun <S : Segment<S>> AtomicRef<S>.moveForward(to: S): Boolean = loop { cur ->
    if (cur.id >= to.id) return true
    if (!to.tryIncPointers()) return false
    if (compareAndSet(cur, to)) { // the segment is moved
        if (cur.decPointers()) cur.remove()
        return true
    }
    if (to.decPointers()) to.remove() // undo tryIncPointers
}

/**
 * Tries to find a segment with the specified [id] following by next references from the
 * [startFrom] segment and creating new ones if needed. The typical use-case is reading this `AtomicRef` values,
 * doing some synchronization, and invoking this function to find the required segment and update the pointer.
 * At the same time, [Segment.cleanPrev] should also be invoked if the previous segments are no longer needed
 * (e.g., queues should use it in dequeue operations).
 *
 * Since segments can be removed from the list, or it can be closed for further segment additions.
 * Returns the segment `s` with `s.id >= id` or `CLOSED` if all the segments in this linked list have lower `id`,
 * and the list is closed.
 */
@Suppress("NOTHING_TO_INLINE")
internal inline fun <S : Segment<S>> AtomicRef<S>.findSegmentAndMoveForward(
    id: Long,
    startFrom: S,
    noinline createNewSegment: (id: Long, prev: S) -> S
): SegmentOrClosed<S> {
    while (true) {
        val s = startFrom.findSegmentInternal(id, createNewSegment)
        if (s.isClosed || moveForward(s.segment)) return s
    }
}
