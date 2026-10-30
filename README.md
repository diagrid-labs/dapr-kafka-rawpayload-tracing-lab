# Trace context for raw Kafka messages in Dapr

A Java app subscribes to a Kafka topic with `rawPayload: "true"`. The producer doesn't use Dapr, so the messages carry no trace context. This lab shows what `traceparent` the app receives on Dapr 1.19.0.

## Run

Needs a cluster with Dapr 1.19.0.

```bash
kubectl apply -f k8s/infra.yaml
kubectl apply -f k8s/dapr.yaml
kubectl -n kafka-tracing-lab create configmap subscriber-src --from-file=RawXmlSubscriber.java
kubectl -n kafka-tracing-lab rollout status deploy/kafka-broker
kubectl -n kafka-tracing-lab exec deploy/kafka-broker -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --topic payments-incoming
kubectl apply -f k8s/apps.yaml -f k8s/sampling-variants.yaml
kubectl -n kafka-tracing-lab wait --for=condition=available deploy --all --timeout=180s
```

Publish one message without headers and one with a `traceparent` Kafka header:

```bash
echo '<PaymentRequest><TxnId>TXN-A-001</TxnId></PaymentRequest>' | \
  kubectl -n kafka-tracing-lab exec -i deploy/kafka-broker -- /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic payments-incoming

printf 'traceparent:00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\t<PaymentRequest><TxnId>TXN-B-002</TxnId></PaymentRequest>\n' | \
  kubectl -n kafka-tracing-lab exec -i deploy/kafka-broker -- /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic payments-incoming --property parse.headers=true
```

See what each subscriber got:

```bash
for d in sub-rate-1 sub-rate-low sub-no-config sub-rate-0; do
  echo "== $d"; kubectl -n kafka-tracing-lab logs deploy/$d -c app | grep traceId
done
```

## Results

**Message without headers:** what the app receives depends on the tracing config.

| Subscriber | Tracing config | `traceparent` received |
|---|---|---|
| `sub-rate-1` | `samplingRate: "1"` | new trace ID, span exported |
| `sub-rate-low` | `samplingRate: "0.0001"` | new trace ID, not sampled |
| `sub-no-config` | none | all zeros |
| `sub-rate-0` | `samplingRate: "0"` | none |

**Message with a `traceparent` header:** every subscriber gets the producer's trace ID (`4bf92f…`).

**Payload:** in all cases the XML arrives unchanged. It's base64-encoded in the `data_base64` field of the request body.

If an app gets no `traceparent` at all, check the daprd log for the version and the sampling rate:

```bash
kubectl -n kafka-tracing-lab logs deploy/sub-rate-0 -c daprd | grep -E 'Starting Dapr Runtime|trace sampler'
```

## Clean up

```bash
kubectl delete namespace kafka-tracing-lab
```
