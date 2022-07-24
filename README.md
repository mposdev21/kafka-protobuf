# kafka-protobuf
Example of how to deal with messages with multiple protobuf schemas on a single kafka topic using shcema registry.

This example shows how to handle multiple schemas using specific classes to deserialize the message instead of using DynamicMessage. This requires that you instruct schema registry to deserialize using specific classes using DERIVE_TYPE_CONFIG. There will be a SerializationException if the underlying class is not linked with the application at runtime.

## Steps to run the application

# Setup Kafka environment
```docker-compose up -d ```

# Run the producer
``` mvn compile exec:java -Dexec.mainClass="com.example.kafka.protobuf.producer.ProtoProducer" ```

You will see the following output:
```
Sent simple {
  index: 4
  tstamp {
    seconds: 1658703708
    nanos: 999000000
  }
}
other {
  name: "other"
  record: 2
}

-------------------------
Sent name: "other"
record: 2

-------------------------
Sent name: "generic"

-------------------------
```
# Run the consumer
``` mvn compile exec:java -Dexec.mainClass="com.example.kafka.protobuf.consumer.ProtoConsumer" ```

You will see the following output:
```
com.example.protobuf.BasicProtos$BasicMessage
Received SimpleMessage (specific support) index: 4
tstamp {
  seconds: 1658703708
  nanos: 999000000
}

com.example.protobuf.OtherProtos$OtherMessage
Received OtherMessage (specific support) name: "other"
record: 2

com.example.protobuf.GenericProtos$GenericMessage
Received Message (handling using DynamicMessage)
name: generic

