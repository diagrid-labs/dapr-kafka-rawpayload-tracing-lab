# Trace context for raw Kafka messages in Dapr

Two Java apps subscribe to a Kafka topic with `rawPayload: "true"`. The producer doesn't use Dapr, so the XML messages carry no trace context. On Dapr 1.19.0, both apps still get a `traceparent` for every message, and the payload stays untouched.

- `RawXmlSubscriber.java`: plain Java, no dependencies
- `spring-sdk-subscriber/`: Spring Boot with the Dapr Java SDK 1.19.0 (`@Topic`)

## Run

Needs the Dapr CLI, Docker, Java 17+ and Maven.

```bash
dapr init --runtime-version 1.19.0
docker compose up -d
docker exec kafka-tracing-lab /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --create --if-not-exists --topic payments-incoming
mvn -q -f spring-sdk-subscriber/pom.xml package -DskipTests
dapr run -f .
```

In another terminal, publish one message without headers and one with a `traceparent` Kafka header:

```bash
echo '<PaymentRequest><TxnId>TXN-A-001</TxnId><Amount>1500.00</Amount></PaymentRequest>' | \
  docker exec -i kafka-tracing-lab /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic payments-incoming

printf 'traceparent:00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\t<PaymentRequest><TxnId>TXN-B-002</TxnId><Amount>2750.00</Amount></PaymentRequest>\n' | \
  docker exec -i kafka-tracing-lab /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic payments-incoming --property parse.headers=true
```

The app output shows up in the `dapr run` terminal, and the traces in Zipkin at http://localhost:9411.

## Result

`raw-subscriber`:

```
traceId=ea708a26f913747bc72c2f24df9111f4 transactionId=TXN-A-001 traceparent=00-ea708a26f913747bc72c2f24df9111f4-7efb681ca1e4c3c2-01 xml=<PaymentRequest><TxnId>TXN-A-001</TxnId><Amount>1500.00</Amount></PaymentRequest>
traceId=4bf92f3577b34da6a3ce929d0e0e4736 transactionId=TXN-B-002 traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-62f43b4078cb5ee5-01 xml=<PaymentRequest><TxnId>TXN-B-002</TxnId><Amount>2750.00</Amount></PaymentRequest>
```

`spring-sdk-subscriber`:

```
header traceparent=00-8aa1a79326dc66c2b54e1d1377e7f967-c80e97c81809a40f-01 cloudevent traceparent=null xml=<PaymentRequest><TxnId>TXN-A-001</TxnId><Amount>1500.00</Amount></PaymentRequest>
header traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-3a8de25d5d46ae6e-01 cloudevent traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01 xml=<PaymentRequest><TxnId>TXN-B-002</TxnId><Amount>2750.00</Amount></PaymentRequest>
```

- **No headers:** Dapr starts a new trace and sends it in the `traceparent` HTTP header.
- **`traceparent` Kafka header:** Dapr continues the producer's trace (`4bf92f…`).
- **Payload:** the XML arrives exactly as published.
- **Read the trace from the HTTP header.** With the Java SDK, `cloudEvent.getTraceParent()` only carries a `traceparent` that came in on the Kafka message; it is `null` for the message without headers.

## Clean up

```bash
dapr stop -f .
docker compose down
```
