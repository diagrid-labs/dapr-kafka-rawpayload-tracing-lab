package io.diagrid.labs.rawpayload;

import io.dapr.Topic;
import io.dapr.client.domain.CloudEvent;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentsController {

  private static final Logger log = LoggerFactory.getLogger(PaymentsController.class);

  // rawPayload=true: the producer did not use Dapr, so the record value is plain XML.
  @Topic(name = "payments-incoming", pubsubName = "kafka-pubsub", metadata = "{\"rawPayload\":\"true\"}")
  @PostMapping("/payments")
  public void handle(@RequestHeader(value = "traceparent", required = false) String traceparent,
                     @RequestBody CloudEvent<byte[]> event) {
    // The original Kafka record value is in data_base64.
    String xml = new String(event.getBinaryData(), StandardCharsets.UTF_8);

    log.info("header traceparent={} cloudevent traceparent={} xml={}",
        traceparent, event.getTraceParent(), xml);
  }
}
