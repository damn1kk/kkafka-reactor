package kkafka

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.whenever
import java.util.concurrent.atomic.AtomicInteger

class OffsetStateTest {
    @Test
    fun `should consume offsets and commit them in sequence`() {
        val state = OffsetState<String, ByteArray>()
        val tp = TopicPartition("topic", 0)

        state.consumeOffset(createMockRecords(tp, listOf(0L, 1L, 2L, 3L, 4L)))
        assertEquals(5, state.getRecordsInProgress())

        state.ack(tp, 0L)
        state.ack(tp, 1L)
        state.ack(tp, 2L)
        state.ack(tp, 3L)
        state.ack(tp, 4L)
        assertEquals(5, state.getOffsetsToCommitCount())

        val offsetsToCommit = state.getOffsetsToCommit()
        assertEquals(5, offsetsToCommit.batchSize)
        assertNotNull(offsetsToCommit.offsetsToCommit[tp])
        assertEquals(5L, offsetsToCommit.offsetsToCommit[tp]?.offset())

        assertEquals(0, state.getOffsetsToCommitCount())
    }

    @Test
    fun `should correctly track records in progress`() {
        val state = OffsetState<String, ByteArray>()
        val tp = TopicPartition("topic", 0)

        assertEquals(0, state.getRecordsInProgress())

        state.consumeOffset(createMockRecords(tp, listOf(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)))
        assertEquals(10, state.getRecordsInProgress())

        state.releaseOffsets(5)
        assertEquals(5, state.getRecordsInProgress())

        state.releaseOffsets(10)
        assertEquals(-5, state.getRecordsInProgress(), "Records in progress can go negative")
    }

    @Test
    fun `should handle partial acknowledgment`() {
        val state = OffsetState<String, ByteArray>()
        val tp = TopicPartition("topic", 0)

        state.consumeOffset(createMockRecords(tp, listOf(0L, 1L, 2L, 3L, 4L)))

        state.ack(tp, 0L)
        state.ack(tp, 2L)
        state.ack(tp, 4L)

        val offsetsToCommit = state.getOffsetsToCommit()
        assertEquals(1, offsetsToCommit.batchSize)
        assertTrue(offsetsToCommit.offsetsToCommit.containsKey(tp))
    }

    @Test
    fun `should handle out of order acks across multiple commits`() {
        val state = OffsetState<String, ByteArray>()
        val tp = TopicPartition("topic", 0)

        state.consumeOffset(createMockRecords(tp, listOf(0L, 1L, 2L, 3L, 4L)))

        state.ack(tp, 2L)
        state.ack(tp, 4L)

        val commit1 = state.getOffsetsToCommit()
        assertEquals(0, commit1.batchSize)

        state.ack(tp, 0L)
        state.ack(tp, 1L)
        state.ack(tp, 3L)

        val commit2 = state.getOffsetsToCommit()
        assertEquals(5, commit2.batchSize)
        assertEquals(5L, commit2.offsetsToCommit[tp]?.offset())
    }

    @Test
    fun `should handle empty records`() {
        val state = OffsetState<String, ByteArray>()
        val tp = TopicPartition("topic", 0)

        state.consumeOffset(createMockRecords(tp, emptyList()))

        assertEquals(0, state.getRecordsInProgress())
        assertEquals(0, state.getOffsetsToCommitCount())

        val offsetsToCommit = state.getOffsetsToCommit()
        assertEquals(0, offsetsToCommit.batchSize)
        assertTrue(offsetsToCommit.offsetsToCommit.isEmpty())
    }

    @Test
    fun `should fail with concurrent modification when multiple threads call getOffsetsToCommit concurrently`() {
        val state = OffsetState<String, ByteArray>()
        val tp = TopicPartition("topic", 0)

        state.consumeOffset(createMockRecords(tp, List(100) { it.toLong() }))
        for (i in 0 until 100) {
            state.ack(tp, i.toLong())
        }
        assertEquals(100, state.getOffsetsToCommitCount())

        val totalCommitted = AtomicInteger(0)

        val threads = List(10) {
            Thread {
                runCatching {
                    repeat(10) {
                        val result = state.getOffsetsToCommit()
                        totalCommitted.addAndGet(result.batchSize)
                    }
                }
            }
        }

        threads.forEach { it.start() }
        threads.forEach { it.join() }

        assertEquals(
            100,
            totalCommitted.get() + state.getOffsetsToCommitCount(),
            "Data corruption detected - total committed + remaining should equal 100",
        )
    }

    @Test
    fun `should handle multiple topic partitions`() {
        val state = OffsetState<String, ByteArray>()
        val tp1 = TopicPartition("topic1", 0)
        val tp2 = TopicPartition("topic2", 0)

        state.consumeOffset(createMockRecords(tp1, listOf(0L, 1L, 2L)))
        state.consumeOffset(createMockRecords(tp2, listOf(0L, 1L)))

        assertEquals(5, state.getRecordsInProgress())

        state.ack(tp1, 0L)
        state.ack(tp1, 1L)
        state.ack(tp1, 2L)
        state.ack(tp2, 0L)
        state.ack(tp2, 1L)

        assertEquals(5, state.getOffsetsToCommitCount())

        val offsetsToCommit = state.getOffsetsToCommit()
        assertEquals(5, offsetsToCommit.batchSize)
        assertEquals(2, offsetsToCommit.offsetsToCommit.size)
    }

    private fun createMockRecords(tp: TopicPartition, offsets: List<Long>): ConsumerRecords<String, ByteArray> {
        val records = offsets.map { offset ->
            val record = mock(org.apache.kafka.clients.consumer.ConsumerRecord::class.java)
            whenever(record.offset()).thenReturn(offset)
            record
        }

        val mockRecords = mock(ConsumerRecords::class.java)
        whenever(mockRecords.count()).thenReturn(offsets.size)
        whenever(mockRecords.partitions()).thenReturn(setOf(tp))
        whenever(mockRecords.records(tp)).thenReturn(records)

        @Suppress("UNCHECKED_CAST")
        return mockRecords as ConsumerRecords<String, ByteArray>
    }
}
