import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal Dapr subscriber for a Kafka topic written by an external producer.
 * JDK only (no dependencies), run with: java RawXmlSubscriber.java
 */
public class RawXmlSubscriber {

    static final String PUBSUB = "kafka-pubsub";
    static final String TOPIC = "payments-incoming";

    static final Pattern DATA_BASE64 = Pattern.compile("\"data_base64\"\\s*:\\s*\"([^\"]*)\"");
    static final Pattern TXN_ID = Pattern.compile("<TxnId>([^<]+)</TxnId>");

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

        // Programmatic subscription. rawPayload=true: the producer did not use Dapr,
        // so the record value is plain XML, not a CloudEvent.
        server.createContext("/dapr/subscribe", ex -> respond(ex, "[{"
                + "\"pubsubname\":\"" + PUBSUB + "\","
                + "\"topic\":\"" + TOPIC + "\","
                + "\"route\":\"/payments\","
                + "\"metadata\":{\"rawPayload\":\"true\"}}]"));

        server.createContext("/payments", ex -> {
            // The trace context arrives as a W3C traceparent HTTP header:
            // 00-<32 hex trace id>-<16 hex span id>-<flags>
            String traceparent = ex.getRequestHeaders().getFirst("traceparent");
            String traceId = traceparent == null ? "<none>" : traceparent.split("-")[1];

            // With rawPayload, Dapr wraps the untouched Kafka record value in
            // data_base64. Decoding it gives back the exact bytes the producer wrote.
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Matcher m = DATA_BASE64.matcher(body);
            String xml = m.find()
                    ? new String(Base64.getDecoder().decode(m.group(1)), StandardCharsets.UTF_8)
                    : body;

            Matcher txn = TXN_ID.matcher(xml);
            String transactionId = txn.find() ? txn.group(1) : "<unknown>";

            // In a Spring/Logback app put these in the MDC instead, e.g.
            // MDC.put("traceId", traceId); MDC.put("transactionId", transactionId);
            System.out.printf("traceId=%s transactionId=%s traceparent=%s xml=%s%n",
                    traceId, transactionId, traceparent, xml);

            respond(ex, "{\"status\":\"SUCCESS\"}");
        });

        server.start();
        System.out.println("RawXmlSubscriber listening on :8080");
    }

    static void respond(HttpExchange ex, String json) throws IOException {
        byte[] out = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(200, out.length);
        ex.getResponseBody().write(out);
        ex.close();
    }
}
