# kkafka-reactor

Kotlin Kafka Reactor Client - A Kotlin library that provides a reactive wrapper around Apache Kafka's consumer API using Project Reactor. It enables non-blocking, reactive consumption of Kafka messages, allowing developers to build reactive streaming applications with Kafka.

## Features

- Non-blocking, reactive consumption of Kafka messages
- Backpressure handling via Reactor Sinks
- Automatic offset tracking and management
- Manual offset acknowledgment and commits
- Pause/resume consumer functionality
- Support for consumer rebalance events

## Requirements

- Kotlin 2.3.10+
- Java 21+
- Apache Kafka Clients 4.2.0
- Reactor Core 3.8.3

## Installation

Add the dependency to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("org.kkafka:kkafka-reactor:1.0-SNAPSHOT")
}
```

## Quick Start

### Basic Usage

```kotlin
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteArrayDeserializer

val props = mapOf(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG to "my-group",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
)

val reactiveClient = ReactiveClient<String, ByteArray>(props, listOf("my-topic"))

reactiveClient.start()
    .subscribe { records ->
        records.forEach { record ->
            val key = record.key()
            val value = String(record.value())
            println("Received: key=$key, value=$value")
        }
    }
```

### Acknowledging Messages

```kotlin
import org.apache.kafka.common.TopicPartition

reactiveClient.start()
    .subscribe { records ->
        records.forEach { record ->
            val tp = TopicPartition(record.topic(), record.partition())
            reactiveClient.ack(tp, record.offset())
        }
    }
```

### Pause and Resume

```kotlin
val reactiveClient = ReactiveClient<String, ByteArray>(props, listOf("my-topic"))

// Start consuming
reactiveClient.start()
    .subscribe { records ->
        // Process records
    }

// Pause consumption
reactiveClient.pause()

// Resume consumption
reactiveClient.resume()
```

## API Reference

### ReactiveClient<K, V>

| Method | Description |
|--------|-------------|
| `start(): Flux<ConsumerRecords<K, V>>` | Starts the consumer and returns a Flux that emits ConsumerRecords |
| `pause()` | Pauses the consumer, preventing new records from being emitted |
| `resume()` | Resumes the consumer after a pause |
| `ack(topicPartition, offset)` | Acknowledges a message at the given offset |
| `commit()` | Manually triggers an offset commit |

### Constructor

```kotlin
// From config properties and topics
ReactiveClient(configProperties: Map<String, Any>, topics: Collection<String>)

// From existing KafkaConsumer
ReactiveClient(consumer: KafkaConsumer<K, V>, topics: Collection<String>)
```

## Configuration

### Consumer Properties

| Property | Description | Required |
|----------|-------------|----------|
| `bootstrap.servers` | Kafka broker addresses | Yes |
| `group.id` | Consumer group ID | Yes |
| `key.deserializer` | Key deserializer class | Yes |
| `value.deserializer` | Value deserializer class | Yes |
| `auto.offset.reset` | Offset reset strategy (earliest/latest) | Yes |

### Internal Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `pollTimeout` | 1000ms | Timeout for each poll operation |
| `commitBatchSize` | 5 | Number of messages before auto-commit |
| `commitInterval` | 5000ms | Interval for periodic commits |

## Testing

The project uses JUnit 5 with Testcontainers for integration testing. A Kafka container is automatically started for tests.

```bash
# Run all tests
./gradlew test

# Run a specific test
./gradlew test --tests "org.kkafka.ReactiveClientTest.should consume messages from topic"

# Run tests with verbose output
./gradlew test --info
```

## License

Licensed under the [MIT License](LICENSE).
