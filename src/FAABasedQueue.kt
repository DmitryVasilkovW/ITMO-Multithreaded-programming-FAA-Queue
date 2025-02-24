import java.util.concurrent.atomic.*

/**
 * @author TODO: Last Name, First Name
 *
 * TODO: Copy the code from `FAABasedQueueSimplified`
 * TODO: and implement the infinite array on a linked list
 * TODO: of fixed-size `Segment`s.
 */
class FAABasedQueue<E> : Queue<E> {

    override fun enqueue(element: E) {
        TODO("Implement me!")
    }

    override fun dequeue(): E? {
        TODO("Implement me!")
    }
}

private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2
