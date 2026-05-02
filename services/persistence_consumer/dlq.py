"""Dead-letter-queue publisher.

Envelopes are JSON rather than protobuf because:
1. They carry the original (possibly unparseable) bytes; proto-wrapping
   an unparseable byte blob in another proto adds no debuggability.
2. DLQ messages are inspected by hand or by small ad-hoc tools, not by
   long-lived consumers with a schema-evolution story.

Key preservation: the DLQ message uses the same Kafka key as the source
message so game-pk partition affinity is preserved. A replayer could
shove DLQ messages back into `mlb.events.raw` without a repartition.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer
from aiokafka.structs import ConsumerRecord


class DLQPublisher:
    def __init__(self, producer: AIOKafkaProducer, topic: str):
        self._producer = producer
        self._topic = topic

    async def publish(
        self,
        msg: ConsumerRecord,
        error_class: str,
        error_message: str,
        attempts: int,
    ) -> None:
        envelope = {
            "original_payload_b64": base64.b64encode(msg.value or b"").decode("ascii"),
            "error_class": error_class,
            "error_message": error_message,
            "source_topic": msg.topic,
            "source_partition": msg.partition,
            "source_offset": msg.offset,
            "first_seen_ts": datetime.now(UTC).isoformat(),
            "attempts": attempts,
        }
        await self._producer.send_and_wait(
            self._topic,
            value=json.dumps(envelope).encode("utf-8"),
            key=msg.key,
        )
