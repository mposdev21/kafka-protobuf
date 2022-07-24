package com.example.kafka.protobuf.producer;

import com.example.protobuf.BasicProtos.SimpleMessage;
import com.example.protobuf.BasicProtos.BasicMessage;
import com.example.protobuf.OtherProtos.OtherMessage;
import com.example.protobuf.GenericProtos.GenericMessage;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.Properties;

public class ProtoProducer {

    public static void main(String[] args) {

        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        properties.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put("value.subject.name.strategy", RecordNameStrategy.class.getName());

        Producer<String, BasicMessage> producer = new KafkaProducer<>(properties);
        Producer<String, OtherMessage> producero = new KafkaProducer<>(properties);
        Producer<String, GenericMessage> producerg = new KafkaProducer<>(properties);


        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();

        SimpleMessage.Builder simpleMessage = SimpleMessage.newBuilder()
                        .setIndex(4)
                        .setTstamp(timestamp);

        OtherMessage.Builder otherMessage = OtherMessage.newBuilder()
                        .setName("other")
                        .setRecord(2);

        BasicMessage.Builder basicBuilder =  BasicMessage.newBuilder();
        BasicMessage basicMessage = basicBuilder
                .addSimple(simpleMessage)
                .setOther(otherMessage)
                .build();


        // Send the Basic record
        ProducerRecord<String, BasicMessage> record
                = new ProducerRecord<>("protobuf-topic", null, basicMessage);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();
        System.out.println("Sent " + basicMessage);
        System.out.println("-------------------------");

        // Send the other record
        OtherMessage other = otherMessage.build();
        ProducerRecord<String, OtherMessage> recordo
                = new ProducerRecord<>("protobuf-topic", null, other);

        producero.send(recordo);
        producero.flush();
        System.out.println("Sent " + other);
        System.out.println("-------------------------");

        // Send the Generic record (consumer has no specific support)
        GenericMessage genericMessage = GenericMessage.newBuilder()
                        .setName("generic")
                        .build();

        ProducerRecord<String, GenericMessage> recordg
                = new ProducerRecord<>("protobuf-topic", null, genericMessage);

        producerg.send(recordg);
        producerg.flush();
        System.out.println("Sent " + genericMessage);
        System.out.println("-------------------------");


        //ensures record is sent before closing the producer
        producero.flush();

        producer.close();
        producero.close();
    }

}
