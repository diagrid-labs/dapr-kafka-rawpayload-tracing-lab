# Trace context for raw Kafka messages consumed through Dapr

This lab covers a Kafka topic written by a producer that does not use Dapr. The topic carries plain XML, so the subscription uses `rawPayload: "true"`, and nothing on the message carries trace context.

It answers three questions:

1. Does the subscriber get a `traceparent`? It depends on the Dapr version: 1.18 sends none, 1.19.0 starts a new trace.
2. Can the producer's trace continue into the subscriber without changing the payload? Yes: add a W3C `traceparent` Kafka record header.
3. Why might a 1.19.0 subscriber still see no `traceparent`? Because of the tracing configuration (see [How the tracing config changes the result](#how-the-tracing-config-changes-the-result)).

Verified on minikube with Dapr 1.19.0 installed through Helm, Kafka 3.9.0 (KRaft) and Zipkin 3.

## What's in here

| Path | What it is |
|---|---|
| `RawXmlSubscriber.java` | Java subscriber with no dependencies and no build step (`java RawXmlSubscriber.java`). It subscribes with `rawPayload: "true"`, reads the `traceparent` header, decodes the original XML and logs both. |
| `k8s/infra.yaml` | Namespace `kafka-tracing-lab`, a single-node Kafka and Zipkin. |
| `k8s/dapr.yaml` | The `pubsub.kafka` component and a tracing `Configuration` with `samplingRate: "1"` exporting to Zipkin. |
| `k8s/apps.yaml` | The subscriber twice, with that config: `subscriber-119` on the default daprd 1.19.0, and `subscriber-118` pinned to daprd 1.18.4. |
| `k8s/sampling-variants.yaml` | The subscriber three more times on daprd 1.19.0: no tracing config, `samplingRate: "0"`, and `samplingRate: "0.0001"`. |

Each subscriber has its own app ID, so it reads the topic in its own consumer group and every subscriber receives every message.

## Run it

You need a cluster with Dapr 1.19.0 installed. Add `--context <name>` to each command if your kubeconfig has more than one cluster.

```bash
kubectl apply -f k8s/infra.yaml
kubectl apply -f k8s/dapr.yaml
kubectl -n kafka-tracing-lab create configmap subscriber-src --from-file=RawXmlSubscriber.java
kubectl -n kafka-tracing-lab rollout status deploy/kafka-broker
kubectl -n kafka-tracing-lab exec deploy/kafka-broker -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists --topic payments-incoming
kubectl apply -f k8s/apps.yaml -f k8s/sampling-variants.yaml
```

Wait for the subscribers:

```bash
kubectl -n kafka-tracing-lab wait --for=condition=available deploy --all --timeout=180s
```

Act as the external producer. First, a record with no headers:

```bash
echo '<PaymentRequest><TxnId>TXN-A-001</TxnId><Amount>1500.00</Amount></PaymentRequest>' | \
  kubectl -n kafka-tracing-lab exec -i deploy/kafka-broker -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payments-incoming
```

Then a record with a `traceparent` Kafka header:

```bash
printf 'traceparent:00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\t<PaymentRequest><TxnId>TXN-B-002</TxnId><Amount>2750.00</Amount></PaymentRequest>\n' | \
  kubectl -n kafka-tracing-lab exec -i deploy/kafka-broker -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payments-incoming \
  --property parse.headers=true
```

Read what each subscriber logged:

```bash
for d in subscriber-119 subscriber-118 sub-no-config sub-rate-0 sub-rate-low; do
  echo "== $d"; kubectl -n kafka-tracing-lab logs deploy/$d -c app | grep traceId
done
```

## Results

### 1.19.0 vs 1.18.4 (`samplingRate: "1"`)

`subscriber-119` (daprd 1.19.0):

```
traceId=c100b1ad60d249f18221c500cc1fd132 transactionId=TXN-A-001 traceparent=00-c100b1ad60d249f18221c500cc1fd132-8a9dd2a40d2acbf2-01 xml=<PaymentRequest><TxnId>TXN-A-001</TxnId><Amount>1500.00</Amount></PaymentRequest>
traceId=4bf92f3577b34da6a3ce929d0e0e4736 transactionId=TXN-B-002 traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-0d24f67e99a94192-01 xml=<PaymentRequest><TxnId>TXN-B-002</TxnId><Amount>2750.00</Amount></PaymentRequest>
```

`subscriber-118` (daprd 1.18.4):

```
traceId=<none> transactionId=TXN-A-001 traceparent=null xml=<PaymentRequest><TxnId>TXN-A-001</TxnId><Amount>1500.00</Amount></PaymentRequest>
traceId=4bf92f3577b34da6a3ce929d0e0e4736 transactionId=TXN-B-002 traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-0effb74d6186de59-01 xml=<PaymentRequest><TxnId>TXN-B-002</TxnId><Amount>2750.00</Amount></PaymentRequest>
```

- **Record with no headers:** 1.18.4 sends no `traceparent` and exports no span. 1.19.0 starts a new root span and sends its `traceparent`. This is the 1.19.0 change listed in the [release notes](https://github.com/dapr/dapr/blob/v1.19.0/docs/release_notes/v1.19.0.md?plain=1#L786) ([dapr/dapr#10511](https://github.com/dapr/dapr/pull/10511)).
- **Record with a `traceparent` header:** both versions continue the producer's trace (`4bf92f…`). Zipkin shows a `pubsub/payments-incoming` span whose parent is the producer's span `00f067aa0ba902b7`.
- In every case, the decoded XML is byte-for-byte what the producer wrote.

### How the tracing config changes the result

Same record with no headers, all on daprd 1.19.0:

| Subscriber | Tracing config | `traceparent` the app receives |
|---|---|---|
| `subscriber-119` | `samplingRate: "1"` | valid trace ID, flags `01`, span exported |
| `sub-rate-low` | `samplingRate: "0.0001"` | valid trace ID, flags `00` (not sampled, so no span exported) |
| `sub-no-config` | no `dapr.io/config` annotation | all zeros: `00-00000000000000000000000000000000-0000000000000000-00` |
| `sub-rate-0` | `samplingRate: "0"` | **no header at all** |

So on 1.19.0, a missing `traceparent` header means tracing is set to `samplingRate: "0"`; with any other setting the header is present. A sidecar that is not really on 1.19.0 shows the same missing header (see `subscriber-118`). Both show up in the first lines of the daprd log:

```bash
kubectl -n kafka-tracing-lab logs deploy/sub-rate-0 -c daprd | grep -E 'Starting Dapr Runtime|trace sampler'
```

```
Starting Dapr Runtime -- version 1.19.0 -- commit cd59bfba89c39344bfaca73f1eb7c76ec411c258
Dapr trace sampler initialized: ParentBased{root:TraceIDRatioBased{0},...}
```

With `samplingRate: "0"` or no tracing config, Dapr starts no span of its own. The producer's `traceparent` header still reaches the app unchanged (span ID `00f067aa0ba902b7`), because Dapr passes Kafka record headers through to the app as HTTP headers.

## What the app receives with `rawPayload: "true"`

- The trace context is in the `traceparent` HTTP request header: `00-<trace id>-<span id>-<flags>`. The trace ID is the second field.
- Kafka record headers and metadata also arrive as HTTP headers (`__key`, `__offset`, `__partition`, `__topic`, `__timestamp`, plus any producer headers).
- The body is a small CloudEvent wrapper. Its `data_base64` field holds the original Kafka record value; base64-decode it to get the XML back.

## Clean up

```bash
kubectl delete namespace kafka-tracing-lab
```
