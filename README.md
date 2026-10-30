# Trace context for raw Kafka messages in Dapr

A Java app subscribes to a Kafka topic with `rawPayload: "true"`. The producer doesn't use Dapr, so the XML messages carry no trace context. On Dapr 1.19.0, the app still gets a `traceparent` for every message, and the payload stays untouched.

## Run

Needs a cluster with Dapr 1.19.0.

```bash
kubectl apply -f k8s/infra.yaml
kubectl apply -f k8s/dapr.yaml
kubectl -n kafka-tracing-lab create configmap subscriber-src --from-file=RawXmlSubscriber.java
kubectl -n kafka-tracing-lab rollout status deploy/kafka-broker
kubectl -n kafka-tracing-lab exec deploy/kafka-broker -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --topic payments-incoming
kubectl apply -f k8s/apps.yaml
kubectl -n kafka-tracing-lab rollout status deploy/subscriber
```

Publish one message without headers, and one with a `traceparent` Kafka header:

```bash
echo '<PaymentRequest><TxnId>TXN-A-001</TxnId><Amount>1500.00</Amount></PaymentRequest>' | \
  kubectl -n kafka-tracing-lab exec -i deploy/kafka-broker -- /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic payments-incoming

printf 'traceparent:00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\t<PaymentRequest><TxnId>TXN-B-002</TxnId><Amount>2750.00</Amount></PaymentRequest>\n' | \
  kubectl -n kafka-tracing-lab exec -i deploy/kafka-broker -- /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic payments-incoming --property parse.headers=true
```

```bash
kubectl -n kafka-tracing-lab logs deploy/subscriber -c app | grep traceId
```

## Result

```
traceId=927a61b1754604ff32f8037a9595dae9 transactionId=TXN-A-001 traceparent=00-927a61b1754604ff32f8037a9595dae9-d7871092198769b5-01 xml=<PaymentRequest><TxnId>TXN-A-001</TxnId><Amount>1500.00</Amount></PaymentRequest>
traceId=4bf92f3577b34da6a3ce929d0e0e4736 transactionId=TXN-B-002 traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-90af940adb4f64c0-01 xml=<PaymentRequest><TxnId>TXN-B-002</TxnId><Amount>2750.00</Amount></PaymentRequest>
```

- **No headers:** Dapr starts a new trace, and the app logs its trace ID next to the transaction ID.
- **`traceparent` header:** Dapr continues the producer's trace (`4bf92f…`).
- **Payload:** the XML arrives exactly as published.

## Clean up

```bash
kubectl delete namespace kafka-tracing-lab
```
