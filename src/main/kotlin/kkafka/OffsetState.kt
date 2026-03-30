package kkafka

import kkafka.log.Log
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.SortedSet
import java.util.TreeSet
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.forEach
import kotlin.concurrent.write

class OffsetState<K, V> {
    data class OffsetsToCommit(
        val offsetsToCommit: Map<TopicPartition, OffsetAndMetadata>,
        val batchSize: Int,
    )

    companion object : Log

    private val consumedOffsets: MutableMap<TopicPartition, SortedSet<Long>> = mutableMapOf()
    private val recordsInProgress = AtomicInteger()
    private val offsetsToCommit: MutableMap<TopicPartition, SortedSet<Long>> = mutableMapOf()
    private val offsetsToCommitCount = AtomicInteger()
    private val offsetsToCommitLock = ReentrantReadWriteLock()

    fun consumeOffset(records: ConsumerRecords<K, V>) {
        records.partitions().forEach { tp ->
            val offsets = consumedOffsets.computeIfAbsent(tp) { TreeSet() }
            records.records(tp).forEach { r -> offsets.add(r.offset()) }
        }
        recordsInProgress.addAndGet(records.count())
    }

    fun releaseOffsets(batchSize: Int) {
        recordsInProgress.addAndGet(-1 * batchSize)
    }

    fun restoreOffsets(offsetsToRestore: Map<TopicPartition, OffsetAndMetadata>) {
        offsetsToRestore.forEach { (tp, offset) ->
            val offsets = consumedOffsets.computeIfAbsent(tp) { TreeSet() }
            offsets.add(offset.offset() - 1)
        }
    }

    fun getRecordsInProgress() = recordsInProgress.get()

    fun getOffsetsToCommitCount(): Int = offsetsToCommitCount.get()

    fun ack(topicPartition: TopicPartition, offset: Long): Int {
        offsetsToCommitLock.write {
            val offsetsForTopicPartition = offsetsToCommit.computeIfAbsent(topicPartition) { TreeSet() }
            offsetsForTopicPartition.add(offset)
            val offsetsToCommitCount = offsetsToCommitCount.incrementAndGet()
            log.debug("getOffsetsToCommitCount increment {} \n {}", getOffsetsToCommitCount(), offsetsToCommit)
            return offsetsToCommitCount
        }
    }

    fun getOffsetsToCommit(): OffsetsToCommit {
        offsetsToCommitLock.write {
            val result = mutableMapOf<TopicPartition, OffsetAndMetadata>()
            var batchSize = 0
            consumedOffsets.forEach { (tp, offsets) ->
                offsetsToCommit.computeIfPresent(tp) { _, offsetsTp ->
                    val toCommitIterator = offsetsTp.iterator()
                    val unCommitedIterator = offsets.iterator()
                    while (unCommitedIterator.hasNext() && toCommitIterator.hasNext()) {
                        val earliestUncommited = unCommitedIterator.next()
                        val earliestToCommit = toCommitIterator.next()
                        if (earliestToCommit != earliestUncommited) {
                            break
                        }
                        toCommitIterator.remove()
                        unCommitedIterator.remove()
                        offsetsToCommitCount.decrementAndGet()
                        batchSize++
                        if (earliestToCommit >= 0) {
                            result[tp] = OffsetAndMetadata(earliestToCommit + 1)
                        }
                    }
                    offsetsTp
                }
            }
            log.debug("getOffsetsToCommitCount decrement ${getOffsetsToCommitCount()}")
            return OffsetsToCommit(result, batchSize)
        }
    }
}
