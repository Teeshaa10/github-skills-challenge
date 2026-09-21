# AIOps Service Monitoring Simulation

## Scenario

This project monitors a synthetic `payment-service`. The operational problem is that slow
payment requests and resource saturation can lead to timeouts and database connection failures.
The AIOps workflow detects those signals and moves an actionable anomaly event through a small
in-memory event-streaming simulation.

AIOps connects operational data to automated detection and event handling. In this assessment the
workflow is:

`Operational data -> anomaly detector -> event -> producer -> topic -> consumer -> AIOps output`

No Kafka, Airflow, or cloud service is required.

## Repository Components

- `data/service_data.json`: ten timestamped `payment-service` telemetry records.
- `src/anomaly_detector.py`: threshold-based metric and log anomaly detection.
- `src/event_producer.py`: publishes detected events.
- `src/event_topic.py`: in-memory topic that stores published events.
- `src/event_consumer.py`: reads events from the topic.
- `src/aiops_pipeline.py`: loads data, detects anomalies, publishes them, consumes them, and
  prints the downstream AIOps result.
- `tests/test_aiops_pipeline.py`: detector and producer/topic/consumer workflow tests.
- `tests/calculations_test.py`: existing calculation validation tests.

## Operational Data Analysis

Each record contains:

- Metrics: `response_time_ms`, `cpu_percent`, and `memory_percent`.
- Log information: `log_level` and `message`.
- Context: `timestamp` and `service`.

Timestamps are ISO-like values at one-minute intervals from `10:00` through `10:09`. They provide
the order of observations and make the incident easy to place in time.

The records from `10:00` through `10:04` and `10:07` through `10:09` represent normal behaviour:
response time is 120-150 ms, CPU is 42-50%, memory is 51-57%, and the log level is `INFO`.

The records at `10:05` and `10:06` are unusual:

| Timestamp | Response | CPU | Memory | Log | Message |
| --- | ---: | ---: | ---: | --- | --- |
| 2026-09-20 10:05 | 610 ms | 75% | 70% | ERROR | Payment service timeout |
| 2026-09-20 10:06 | 640 ms | 94% | 91% | ERROR | Database connection timeout |

The second record is the most severe because all three metric thresholds are exceeded as well as
the log being an error.

## Detection Findings

The detector uses these strict thresholds: response time greater than 500 ms, CPU greater than
80%, memory greater than 80%, and a `WARNING` or `ERROR` log level. It returns `None` for normal
records and an `ANOMALY` event containing the timestamp, service, original source record, and
human-readable reasons for anomalous records.

The final run detected both expected anomalies and did not flag any normal record:

- `10:05`: high response time and concerning `ERROR` log level.
- `10:06`: high response time, high CPU, high memory, and concerning `ERROR` log level.

No expected anomaly was missed and no normal event was incorrectly flagged in this dataset. The
main limitation is that fixed thresholds do not adapt to changing traffic or service baselines.
A useful improvement would be a rolling baseline or seasonality-aware detector, supplemented with
tests for boundary values and additional log patterns.

## Event Flow

For each detected event, `EventProducer.publish` sends the event to the shared in-memory
`anomaly-events` `EventTopic`. `EventConsumer.consume` reads the topic messages, which are then
used as the final AIOps output by the pipeline. The event preserves the source telemetry and
reasons, so an operator can see both what happened and why it was flagged.

Two workflow issues were identified and corrected:

1. The detector checked for `WARNING` only, so the supplied `ERROR` log signal was not reported.
	It now reports both `WARNING` and `ERROR` as concerning log levels.
2. The producer and consumer were connected to different topics, so detected events were never
	consumed. Both now use the same `anomaly-events` topic.
3. The modules used only script-style imports, which made the test import of `src.aiops_pipeline`
	fail in a clean environment. Compatible package/script imports now support both execution
	modes without changing the event architecture.

## Reproduce the Demonstration

From the repository root:

```bash
python3 -m pip install -r requirements.txt
python3 src/aiops_pipeline.py
python3 -m pytest -q
```

Expected pipeline output includes:

```text
Records processed: 10
Anomalies detected: 2
Events consumed: 2
```

The two consumed events should have timestamps `2026-09-20T10:05:00` and
`2026-09-20T10:06:00`, with the reasons listed above. The test suite validates normal-record
filtering, anomaly detection, event publication, topic consumption, and complete pipeline
delivery.

## Validation Evidence

The final execution provides evidence for the required stages: `service_data.json` is the
operational input, the printed reasons are the anomaly-detection report, `Events consumed: 2`
demonstrates producer/topic/consumer delivery, and the detailed consumed events are the final
AIOps output. The successful `python3 -m pytest -q` run validates the workflow automatically.

