package kkafka

import kkafka.log.Log
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.FluxSink
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ReactiveClient<K, V>(
    private val consumer: KafkaConsumer<K, V>,
    private val topics: Collection<String>,
) {
    constructor(configProperties: Map<String, Any>, topics: Collection<String>) : this(
        KafkaConsumer(configProperties),
        topics,
    )

    companion object : Log

    private val isRunning = AtomicBoolean(false)
    private val state: OffsetState<K, V> = OffsetState()
    private val pollTimeout: Duration = Duration.ofMillis(1000)
    private val commitBatchSize: Int = 5
    private val commitInterval: Duration = Duration.ofMillis(5000)
    private val isPaused = AtomicBoolean(false)

    private lateinit var periodicallyCommitDisposable: Disposable

    val scheduler: Scheduler = Schedulers.newSingle("groupIdScheduler")

    private fun validateConnection(): Throwable? =
        try {
            consumer.listTopics()
            null
        } catch (e: Exception) {
            e
        }

    fun start(): Flux<ConsumerRecords<K, V>> {
        val connectionError = validateConnection()
        if (connectionError != null) {
            return Flux.error(connectionError)
        }

        scheduler.schedule {
            log.info("subscribe topics: $topics")
            consumer.subscribe(topics, RebalanceListener())
        }
        periodicallyCommitDisposable =
            Schedulers.parallel().schedulePeriodically(
                { scheduler.schedule { commitEvent() } },
                commitInterval.toMillis(),
                commitInterval.toMillis(),
                TimeUnit.MILLISECONDS,
            )

        return Flux
            .create { sink: FluxSink<ConsumerRecords<K, V>> ->
                isRunning.set(true) //

                val pollTask =
                    scheduler.schedulePeriodically(
                        {
                            log.debug("Start poll task")
                            if (!isRunning.get() || sink.isCancelled) {
                                return@schedulePeriodically
                            }
                            kotlin
                                .runCatching {
                                    val requested = sink.requestedFromDownstream()
                                    log.debug("requested: $requested")
                                    if (requested > 0) {
                                        val records = consumer.poll(pollTimeout)
                                        if (!records.isEmpty) {
                                            log.debug("Emitting ${records.count()}")
                                            state.consumeOffset(records)
                                            sink.next(records)
                                        }
                                    }
                                }.onFailure { ex ->
                                    log.error("Got error during polling", ex)
                                    if (!sink.isCancelled) {
                                        sink.error(ex)
                                    }
                                }
                        },
                        0,
                        pollTimeout.toMillis(),
                        TimeUnit.MILLISECONDS,
                    )

                sink.onCancel {
                    isRunning.set(false)
                    pollTask.dispose()
                    periodicallyCommitDisposable.dispose()
                    scheduler.schedule { consumer.close() }
                }

                sink.onDispose {
                    isRunning.set(false)
                    pollTask.dispose()
                    periodicallyCommitDisposable.dispose()
                    scheduler.schedule { consumer.close() }
                }
            }.doOnError { error ->
                log.error("Got error: ${error.message}", error)
                isRunning.set(false)
                scheduler.schedule { consumer.close() }
            }.publishOn(Schedulers.boundedElastic(), 1)
    }

    fun ack(topicPartition: TopicPartition, offset: Long) {
        log.debug("ack: t: ${topicPartition.topic()} p: ${topicPartition.partition()} o: $offset")
        val currentOffsetsToCommitCount = state.ack(topicPartition, offset)
        log.debug("getOffsetsToCommitCount: $currentOffsetsToCommitCount")
        if (commitBatchSize > 0 && currentOffsetsToCommitCount >= commitBatchSize) {
            commit()
        }
    }

    fun commit() {
        scheduler.schedule(commitEvent())
    }

    fun pause() {
        scheduler.schedule(pauseEvent())
    }

    fun resume() {
        scheduler.schedule(resumeEvent())
    }

    private fun commitEvent(): Runnable =
        Runnable {
            log.debug("commit event try getOffsetsToCommit")
            val offsetsToCommit = state.getOffsetsToCommit()
            log.debug("try async commit: {}", offsetsToCommit)
            consumer.commitAsync(offsetsToCommit.offsetsToCommit) { offsets, error ->
                log.debug("Async commit finished")
                if (error != null) {
                    state.restoreOffsets(offsetsToCommit.offsetsToCommit)
                    log.error("error during committing: $offsets. error.message: ${error.message}", error)
                } else {
                    state.releaseOffsets(offsetsToCommit.batchSize)
                    log.debug("Async commit success")
                }
            }
        }

    private fun pauseEvent(): Runnable =
        Runnable {
            if (isPaused.compareAndSet(false, true)) {
                log.warn("pause consumer")
                consumer.pause(consumer.assignment())
            }
        }

    private fun resumeEvent(): Runnable =
        Runnable {
            if (isPaused.compareAndSet(true, false)) {
                log.info("resume consumer")
                consumer.resume(consumer.assignment())
            }
        }

    inner class RebalanceListener : ConsumerRebalanceListener {
        override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
            while (state.getRecordsInProgress() > 0) {
                Thread.sleep(1000)
            }
            log.info("onPartitionsRevoked: $partitions")
        }

        override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
            log.info("onPartitionsAssigned: $partitions")
            if (isPaused.get()) {
                log.info("pause topic partitions: [$partitions]")
                consumer.pause(partitions)
            }
        }

        override fun onPartitionsLost(partitions: MutableCollection<TopicPartition>) {
            super.onPartitionsLost(partitions)
        }
    }
}
