package com.example

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import java.util.*

class MySourceTask : SourceTask() {

    private lateinit var sourceTopic: String
    private lateinit var destTopic: String
    private lateinit var consumer: KafkaConsumer<String, String>
    private lateinit var producer: KafkaProducer<String, String>

    /**
     * NOTE: Task를 시작할 때 호출됩니다.
     * 1. props 로 커스텀 config 만들
     * 2. 프로듀서와 컨슈머 초기화
     * 3. 사용할 변수들 초기화, config to variables
     */
    override fun start(props: Map<String, String>) {
        sourceTopic = props["source.topic"]!!
        destTopic = props["dest.topic"]!!

        val consumerProps = Properties()
        consumerProps["bootstrap.servers"] = "localhost:9092"
        consumerProps["group.id"] = "my-source-connector-group"
        consumerProps["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        consumerProps["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        consumer = KafkaConsumer(consumerProps)
        consumer.subscribe(listOf(sourceTopic))

        val producerProps = Properties()
        producerProps["bootstrap.servers"] = "localhost:9092"
        producerProps["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        producerProps["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        producer = KafkaProducer(producerProps)
    }

    /**
     * Note: 주기적으로 호출되어 데이터를 가져오고 이를 Kafka로 전송합니다.
     * - SourceRecord 리스트를 반환합니다.
     */
    override fun poll(): List<SourceRecord> {
        val records = consumer.poll(1000)
        val sourceRecords = mutableListOf<SourceRecord>()
        for (record in records) {
            producer.send(ProducerRecord(destTopic, record.key(), record.value()))
            val sourcePartition = Collections.singletonMap<String, Any>("sourcePartition", 0)
            val sourceOffset = Collections.singletonMap<String, Any>("sourceOffset", record.offset())
            sourceRecords.add(SourceRecord(sourcePartition, sourceOffset, destTopic, null, record.key(), null, record.value()))
        }
        return sourceRecords
    }

    /**
     * 생명주기: 테스크 종료될 때
     */
    override fun stop() {
        consumer.close()
        producer.close()
    }

    override fun version(): String {
        return "1.0"
    }
}
