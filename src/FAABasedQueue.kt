import java.util.concurrent.atomic.*

/**
 * @author Vasilkov Dmitry
 */
class FAABasedQueue<E> : Queue<E> {
    private val head: AtomicReference<Segment>
    private val tail: AtomicReference<Segment>
    private val enqIdx = AtomicLong(0)
    private val deqIdx = AtomicLong(0)

    init {
        val dummy = Segment(0)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    override fun enqueue(element: E) {
        while (true) {
            val curTail = tail.get()
            val i = enqIdx.getAndIncrement()
            val curSegment = findSegment(begin = curTail, id = i / SEGMENT_SIZE)

            moveForward(tail, src = curTail, dest = curSegment)
            if (cas(curSegment, i, element)) {
                return
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while (true) {
            if (!shouldTryToDequeue()) {
                return null
            }

            val curHead = head.get()
            val i = deqIdx.getAndIncrement()
            val curSegment = findSegment(begin = curHead, id = i / SEGMENT_SIZE)

            moveForward(head, src = curHead, dest = curSegment)
            if (cas(curSegment, i, POISONED)) {
                continue
            }

            return curSegment.cells.getAndSet((i % SEGMENT_SIZE).toInt(), null) as E
        }
    }

    private fun<T> cas(segment: Segment, i: Long, element: T): Boolean {
        return segment.cells.compareAndSet(
            (i % SEGMENT_SIZE).toInt(),
            null,
            element
        )
    }

    private fun findSegment(begin: Segment, id: Long): Segment {
        var cur = begin
        for (curId in begin.id..< id) {
            val next = Segment(curId + 1)
            cur.next.compareAndSet(null, next)
            cur = cur.next.get()!!
        }

        return cur
    }

    private fun moveForward(
        part: AtomicReference<Segment>,
        src: Segment,
        dest: Segment
    ) {
        if (src.id < dest.id) {
            part.compareAndSet(src, dest)
        }
    }

    private fun shouldTryToDequeue(): Boolean {
        while (true) {
            val curDeqIdx = deqIdx.get()
            val curEnqIdx = enqIdx.get()
            if (curDeqIdx == deqIdx.get()) {
                return curDeqIdx < curEnqIdx
            }
        }
    }
}

private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2
private val POISONED = Any()
