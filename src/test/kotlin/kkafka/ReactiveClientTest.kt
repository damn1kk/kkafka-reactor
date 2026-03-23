package kkafka

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.shaded.org.awaitility.Awaitility.await
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Testcontainers
class ReactiveClientTest {
    companion object {
        @Container
        private val kafkaContainer: KafkaContainer = KafkaContainer("apache/kafka:4.2.0")

        @BeforeAll
        @JvmStatic
        fun setup() {
            kafkaContainer.start()
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            kafkaContainer.stop()
        }
    }

    private fun consumerProps(groupId: String = UUID.randomUUID().toString()): Map<String, Any> =
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        )

    private fun producerProps(): Map<String, Any> =
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.CLIENT_ID_CONFIG to "example",
            ProducerConfig.ACKS_CONFIG to "all",
        )

    private fun uniqueTopic() = "test-topic-${UUID.randomUUID()}"

    @Test
    fun `should consume messages from topic`() {
        val topic = uniqueTopic()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(), listOf(topic))

        // Produce some test messages
        val messages = listOf("msg1", "msg2", "msg3")
        for (msg in messages) {
            KafkaProducer<String, ByteArray>(producerProps()).use { producer ->
                producer.send(ProducerRecord(topic, "key", msg.toByteArray(Charsets.UTF_8)))
            }
        }

        // Consume messages
        val receivedMessages = mutableListOf<String>()
        val latch = CountDownLatch(messages.size)

        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    val value = String(record.value(), charset("UTF-8"))
                    receivedMessages.add(value)
                    latch.countDown()
                }
            }, { error ->
                fail("Error during consumption: ${error.message}")
            })

        // Wait for all messages to be consumed
        assertTrue(latch.await(10, TimeUnit.SECONDS))

        // Verify all messages were received
        assertEquals(messages.size, receivedMessages.size)
        assertTrue(receivedMessages.containsAll(messages))
    }

    @Test
    fun `should handle multiple topics`() {
        val topic1 = uniqueTopic()
        val topic2 = uniqueTopic()

        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(), listOf(topic1, topic2))

        // Produce messages to both topics
        KafkaProducer<String, ByteArray>(producerProps()).use { producer ->
            producer.send(ProducerRecord(topic1, "key", "msg-topic1".toByteArray(Charsets.UTF_8)))
            producer.send(ProducerRecord(topic2, "key2", "msg-topic2".toByteArray(Charsets.UTF_8)))
        }

        // Consume from multiple topics
        val receivedMessages = mutableListOf<String>()
        val latch = CountDownLatch(2)

        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    val topic = record.topic()
                    val value = String(record.value(), Charsets.UTF_8)
                    receivedMessages.add("$topic:$value")
                    latch.countDown()
                }
            }, { error ->
                fail("Error during consumption: ${error.message}")
            })

        assertTrue(latch.await(10, TimeUnit.SECONDS))

        // Verify messages from both topics
        assertTrue(receivedMessages.contains("$topic1:msg-topic1"))
        assertTrue(receivedMessages.contains("$topic2:msg-topic2"))
    }

    @Test
    fun `should handle pause and resume`() {
        val topic = uniqueTopic()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(), listOf(topic))
        val producer = KafkaProducer<String, ByteArray>(producerProps())
        // Produce initial message
        producer.send(ProducerRecord(topic, "key", "initial".toByteArray(Charsets.UTF_8)))

        val receivedMessages = mutableListOf<String>()
        val initialMessageLatch = CountDownLatch(1)
        val pausedMessageLatch = CountDownLatch(1)
        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    val recordTopic = record.topic()
                    val value = String(record.value(), Charsets.UTF_8)
                    println("handle message: $value")
                    receivedMessages.add("$recordTopic:$value")
                    if (value == "initial") {
                        // Pause after receiving first message
                        reactiveClient.pause()
                        initialMessageLatch.countDown()
                    } else if (value == "paused") {
                        pausedMessageLatch.countDown()
                    }
                }
            })

        assertTrue(initialMessageLatch.await(10, TimeUnit.SECONDS))
        // Produce message while paused
        producer.send(ProducerRecord(topic, "key", "paused".toByteArray(Charsets.UTF_8)))
        await()
            .during(Duration.ofSeconds(2))
            .until { receivedMessages.size == 1 }

        // Resume and consume
        reactiveClient.resume()

        assertTrue(pausedMessageLatch.await(10, TimeUnit.SECONDS))

        // Verify messages from both topics
        assertTrue(receivedMessages.contains("$topic:initial"))
        assertTrue(receivedMessages.contains("$topic:paused"))
    }

    @Test
    fun `should handle offset commits`() {
        val topic = uniqueTopic()
        val groupId = UUID.randomUUID().toString()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(groupId), listOf(topic))
        val producer = KafkaProducer<String, ByteArray>(producerProps())
        val kafkaConsumer = KafkaConsumer<String, ByteArray>(consumerProps(groupId))

        val messageCount = 5
        val latch = CountDownLatch(messageCount)

        // Produce messages
        for (i in 1..messageCount) {
            producer.send(ProducerRecord(topic, "key", "msg-$i".toByteArray(Charsets.UTF_8)))
        }

        // Start consuming and acknowledge offsets
        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    reactiveClient.ack(TopicPartition(record.topic(), record.partition()), record.offset())
                    latch.countDown()
                }
            })

        // Wait for all messages to be consumed and acknowledged
        assertTrue(latch.await(10, TimeUnit.SECONDS))

        // Trigger manual commit and wait for it
        reactiveClient.commit()
        Thread.sleep(500)

        // Verify committed offsets
        val partitions =
            kafkaConsumer
                .partitionsFor(topic)
                ?.map { TopicPartition(it.topic(), it.partition()) }
                ?.toSet()
                ?: emptySet()

        assertFalse(partitions.isEmpty(), "No partitions found for $topic")

        val committed = kafkaConsumer.committed(partitions)
        var totalCommitted = 0L
        for (tp in partitions) {
            val offset = committed[tp]?.offset()
            assertNotNull(offset, "No committed offset for $tp")
            totalCommitted += offset!!
        }

        assertEquals(messageCount.toLong(), totalCommitted, "Total committed offsets should equal message count")
    }

    @Test
    fun `should handle error during consumption`() {
        val topic = uniqueTopic()
        val groupId = UUID.randomUUID().toString()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(groupId), listOf(topic))

        var fluxCompleted = false
        var fluxError: Throwable? = null
        var receivedCount = 0

        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    receivedCount++
                }
            }, { error ->
                fluxError = error
            }, {
                fluxCompleted = true
            })

        await()
            .during(Duration.ofSeconds(3))
            .until { receivedCount >= 0 }

        assertNull(fluxError, "No error should occur on empty topic")
        assertFalse(fluxCompleted, "Flux should not complete on empty topic")
        assertEquals(0, receivedCount, "No messages should be received from empty topic")
    }

    @Test
    fun `should handle broker disconnection`() {
        val topic = uniqueTopic()
        val brokenProps =
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9999",
                ConsumerConfig.GROUP_ID_CONFIG to UUID.randomUUID().toString(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG to 3000,
                ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG to 1000,
            )

        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(brokenProps, listOf(topic))
        val errorLatch = CountDownLatch(1)

        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    println("Received: ${String(record.value(), Charsets.UTF_8)}")
                }
            }, { error ->
                errorLatch.countDown()
            })

        assertTrue(errorLatch.await(15, TimeUnit.SECONDS), "Error should be propagated on broker disconnection")
    }

    @Test
    fun `should handle deserialization errors`() {
        val topic = uniqueTopic()
        val groupId = UUID.randomUUID().toString()
        val errorLatch = CountDownLatch(1)

        val deserializationErrorProps =
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to IntegerDeserializer::class.java,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            )

        val reactiveClient: ReactiveClient<String, Int> = ReactiveClient(deserializationErrorProps, listOf(topic))

        val stringProducerProps =
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.CLIENT_ID_CONFIG to "example",
                ProducerConfig.ACKS_CONFIG to "all",
            )
        val stringProducer = KafkaProducer<String, String>(stringProducerProps)

        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    println("Received: ${record.value()}")
                }
            }, { error ->
                println("Deserialization error: ${error.message}")
                errorLatch.countDown()
            })

        stringProducer.send(ProducerRecord(topic, "key", "not-a-number"))
        stringProducer.flush()

        assertTrue(errorLatch.await(10, TimeUnit.SECONDS), "Deserialization error should be propagated")
    }

    @Test
    fun `should handle empty topic`() {
        val topic = uniqueTopic()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(), listOf(topic))

        val receivedMessages = mutableListOf<String>()
        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    val value = String(record.value(), Charsets.UTF_8)
                    receivedMessages.add(value)
                }
            }, { error ->
                fail("Error during consumption: ${error.message}")
            })

        await()
            .during(Duration.ofSeconds(5))
            .until { receivedMessages.isEmpty() }

        assertEquals(0, receivedMessages.size)
        reactiveClient.pause()
    }

    @Test
    fun `should handle large batch of messages`() {
        val topic = uniqueTopic()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(), listOf(topic))
        val producer = KafkaProducer<String, ByteArray>(producerProps())

        val messageCount = 100
        val latch = CountDownLatch(messageCount)

        // Produce 100 messages
        for (i in 1..messageCount) {
            producer.send(ProducerRecord(topic, "key", "msg-$i".toByteArray(Charsets.UTF_8)))
        }

        val receivedMessages = mutableListOf<String>()
        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    val value = String(record.value(), charset("UTF-8"))
                    receivedMessages.add(value)
                    latch.countDown()
                }
            })

        assertTrue(latch.await(30, TimeUnit.SECONDS))
        assertEquals(messageCount, receivedMessages.size)
    }

    @Test
    fun `should handle topic partition changes`() {
        val topic = uniqueTopic()
        val adminProps = mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers)
        AdminClient.create(adminProps).use { admin ->
            admin.createTopics(listOf(NewTopic(topic, 3, 1.toShort()))).all().get()
        }

        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(), listOf(topic))
        val producer = KafkaProducer<String, ByteArray>(producerProps())

        val messageCount = 5
        val latch = CountDownLatch(messageCount)

        // Produce messages to multiple partitions
        for (i in 1..messageCount) {
            producer.send(ProducerRecord(topic, i % 3, "key", "msg-$i".toByteArray(Charsets.UTF_8)))
        }
        producer.flush()

        val receivedMessages = mutableListOf<String>()
        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    val value = String(record.value(), charset("UTF-8"))
                    receivedMessages.add(value)
                    latch.countDown()
                }
            })

        assertTrue(latch.await(10, TimeUnit.SECONDS))
        assertTrue(receivedMessages.containsAll(listOf("msg-1", "msg-2", "msg-3", "msg-4", "msg-5")))
    }

    @Test
    fun `should handle message ordering`() {
        val topic = uniqueTopic()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(), listOf(topic))
        val producer = KafkaProducer<String, ByteArray>(producerProps())

        val messageCount = 10
        val latch = CountDownLatch(messageCount)

        // Produce messages in order to single partition for ordering guarantee
        for (i in 1..messageCount) {
            producer.send(ProducerRecord(topic, 0, "key", "msg-$i".toByteArray(Charsets.UTF_8)))
        }
        producer.flush()

        val receivedMessages = mutableListOf<String>()
        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    val value = String(record.value(), charset("UTF-8"))
                    receivedMessages.add(value)
                    latch.countDown()
                }
            })

        assertTrue(latch.await(10, TimeUnit.SECONDS))
        assertEquals(
            listOf("msg-1", "msg-2", "msg-3", "msg-4", "msg-5", "msg-6", "msg-7", "msg-8", "msg-9", "msg-10"),
            receivedMessages,
        )
    }

    @Test
    fun `should trigger auto commit when batch size reached`() {
        val topic = uniqueTopic()
        val groupId = UUID.randomUUID().toString()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(groupId), listOf(topic))
        val producer = KafkaProducer<String, ByteArray>(producerProps())
        val kafkaConsumer = KafkaConsumer<String, ByteArray>(consumerProps(groupId))

        val messageCount = 5
        val latch = CountDownLatch(messageCount)

        for (i in 1..messageCount) {
            producer.send(ProducerRecord(topic, "key", "msg-$i".toByteArray(Charsets.UTF_8)))
        }
        producer.flush()

        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    reactiveClient.ack(TopicPartition(record.topic(), record.partition()), record.offset())
                    latch.countDown()
                }
            })

        assertTrue(latch.await(10, TimeUnit.SECONDS))
        await()
            .during(Duration.ofSeconds(3))
            .untilAsserted {
                val partitions =
                    kafkaConsumer
                        .partitionsFor(topic)
                        ?.map { TopicPartition(it.topic(), it.partition()) }
                        ?.toSet()
                        ?: emptySet()
                val committed = kafkaConsumer.committed(partitions)
                var totalCommitted = 0L
                for (tp in partitions) {
                    totalCommitted += committed[tp]?.offset() ?: 0L
                }
                assertEquals(messageCount.toLong(), totalCommitted)
            }
    }

    @Test
    fun `should handle cancellation gracefully`() {
        val topic = uniqueTopic()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(), listOf(topic))
        val producer = KafkaProducer<String, ByteArray>(producerProps())

        producer.send(ProducerRecord(topic, "key", "msg-1".toByteArray(Charsets.UTF_8)))
        producer.send(ProducerRecord(topic, "key", "msg-2".toByteArray(Charsets.UTF_8)))
        producer.flush()

        val receivedMessages = mutableListOf<String>()
        val disposable =
            reactiveClient
                .start()
                .subscribe({ records ->
                    records.forEach { record ->
                        val value = String(record.value(), Charsets.UTF_8)
                        receivedMessages.add(value)
                    }
                })

        await()
            .during(Duration.ofSeconds(2))
            .until { receivedMessages.isNotEmpty() }

        assertTrue(receivedMessages.isNotEmpty())
        disposable.dispose()
    }

    @Test
    fun `should not consume messages while paused`() {
        val topic = uniqueTopic()
        val reactiveClient: ReactiveClient<String, ByteArray> = ReactiveClient(consumerProps(), listOf(topic))
        val producer = KafkaProducer<String, ByteArray>(producerProps())

        reactiveClient.pause()

        for (i in 1..3) {
            producer.send(ProducerRecord(topic, "key", "msg-$i".toByteArray(Charsets.UTF_8)))
        }
        producer.flush()

        val receivedWhilePaused = mutableListOf<String>()
        val latch = CountDownLatch(3)

        reactiveClient
            .start()
            .subscribe({ records ->
                records.forEach { record ->
                    val value = String(record.value(), charset("UTF-8"))
                    receivedWhilePaused.add(value)
                    latch.countDown()
                }
            })

        await()
            .during(Duration.ofSeconds(2))
            .until { receivedWhilePaused.size == 0 }

        assertEquals(0, receivedWhilePaused.size, "Should not receive messages while paused")

        reactiveClient.resume()

        assertTrue(latch.await(10, TimeUnit.SECONDS))
        assertEquals(3, receivedWhilePaused.size)
    }
}
