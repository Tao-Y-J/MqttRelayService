#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import random
import statistics
import struct
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


DEFAULT_DEPENDENCY_DIR = r"C:\Users\admin\.cache\codex-mqtt-stress\site-packages"


def ensure_dependency_path() -> None:
    dependency_dir = os.environ.get("MQTT_STRESS_PYTHONPATH", DEFAULT_DEPENDENCY_DIR)
    dependency_path = Path(dependency_dir)
    if dependency_path.exists() and str(dependency_path) not in sys.path:
        sys.path.insert(0, str(dependency_path))


ensure_dependency_path()

import paho.mqtt.client as mqtt  # noqa: E402


def reason_code_value(reason_code) -> int:
    return getattr(reason_code, "value", reason_code)


def percentile(values: List[float], ratio: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, max(0, int((len(sorted_values) - 1) * ratio)))
    return sorted_values[index]


def create_payload(payload_size: int, publisher_index: int, sequence: int) -> bytes:
    minimum_size = 16
    final_size = max(minimum_size, payload_size)
    payload = bytearray(final_size)
    struct.pack_into("!Q", payload, 0, time.time_ns())
    struct.pack_into("!I", payload, 8, sequence & 0xFFFFFFFF)
    struct.pack_into("!I", payload, 12, publisher_index & 0xFFFFFFFF)
    if final_size > minimum_size:
        payload[16:] = b"x" * (final_size - minimum_size)
    return bytes(payload)


@dataclass
class SubscriberStats:
    client_id: str
    topic: str
    received: int = 0
    errors: List[str] = field(default_factory=list)
    latencies_ms: List[float] = field(default_factory=list)


@dataclass
class PublisherStats:
    client_id: str
    sent: int = 0
    errors: List[str] = field(default_factory=list)


class SubscriberWorker(threading.Thread):
    def __init__(
        self,
        index: int,
        host: str,
        port: int,
        topic: str,
        timeout_seconds: float,
        keepalive_seconds: int,
        ready_event: threading.Event,
        stop_event: threading.Event,
        latency_sample_rate: int,
        username: Optional[str],
        password: Optional[str],
    ) -> None:
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.timeout_seconds = timeout_seconds
        self.keepalive_seconds = keepalive_seconds
        self.ready_event = ready_event
        self.stop_event = stop_event
        self.latency_sample_rate = max(1, latency_sample_rate)
        self.username = username
        self.password = password
        self.stats = SubscriberStats(
            client_id=f"stress-sub-{index}-{uuid.uuid4().hex[:8]}",
            topic=topic,
        )
        self.client: Optional[mqtt.Client] = None

    def _on_connect(self, client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
        if getattr(reason_code, "is_failure", False) or reason_code_value(reason_code) != 0:
            self.stats.errors.append(f"connect failed: {reason_code}")
            self.ready_event.set()
            return

        result, _ = client.subscribe(self.stats.topic, qos=0)
        if result != mqtt.MQTT_ERR_SUCCESS:
            self.stats.errors.append(f"subscribe call failed: {result}")
            self.ready_event.set()

    def _on_subscribe(self, client: mqtt.Client, userdata, mid, reason_code_list, properties) -> None:
        self.ready_event.set()

    def _on_message(self, client: mqtt.Client, userdata, message: mqtt.MQTTMessage) -> None:
        self.stats.received += 1
        if self.stats.received % self.latency_sample_rate != 0:
            return

        if len(message.payload) < 8:
            self.stats.errors.append("payload too short")
            return

        sent_at_ns = struct.unpack_from("!Q", message.payload, 0)[0]
        latency_ms = (time.time_ns() - sent_at_ns) / 1_000_000
        self.stats.latencies_ms.append(latency_ms)

    def _on_disconnect(self, client: mqtt.Client, userdata, disconnect_flags, reason_code, properties) -> None:
        if self.stop_event.is_set():
            return
        if reason_code_value(reason_code) != 0:
            self.stats.errors.append(f"disconnect: {reason_code}")
            self.ready_event.set()

    def run(self) -> None:
        try:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.stats.client_id,
                protocol=mqtt.MQTTv5,
            )
            self.client = client
            client.enable_logger()
            if self.username is not None:
                client.username_pw_set(self.username, self.password)

            client.on_connect = self._on_connect
            client.on_subscribe = self._on_subscribe
            client.on_message = self._on_message
            client.on_disconnect = self._on_disconnect

            client.connect(self.host, self.port, keepalive=self.keepalive_seconds)
            client.loop_start()

            deadline = time.time() + self.timeout_seconds
            while not self.ready_event.is_set() and time.time() < deadline:
                time.sleep(0.02)

            if not self.ready_event.is_set():
                self.stats.errors.append(f"subscribe not ready within {self.timeout_seconds}s")
                self.ready_event.set()

            while not self.stop_event.is_set():
                time.sleep(0.1)
        except Exception as ex:  # noqa: BLE001
            self.stats.errors.append(str(ex))
            self.ready_event.set()
        finally:
            if self.client is not None:
                try:
                    self.client.disconnect()
                except Exception:  # noqa: BLE001
                    pass
                self.client.loop_stop()


class PublisherWorker(threading.Thread):
    def __init__(
        self,
        index: int,
        host: str,
        port: int,
        timeout_seconds: float,
        keepalive_seconds: int,
        start_event: threading.Event,
        stop_event: threading.Event,
        target_topics: List[str],
        payload_size: int,
        messages: int,
        duration_seconds: float,
        rate_limit_per_second: float,
        username: Optional[str],
        password: Optional[str],
    ) -> None:
        super().__init__(daemon=True)
        self.index = index
        self.host = host
        self.port = port
        self.timeout_seconds = timeout_seconds
        self.keepalive_seconds = keepalive_seconds
        self.start_event = start_event
        self.stop_event = stop_event
        self.target_topics = target_topics
        self.payload_size = payload_size
        self.messages = messages
        self.duration_seconds = duration_seconds
        self.rate_limit_per_second = rate_limit_per_second
        self.username = username
        self.password = password
        self.stats = PublisherStats(client_id=f"stress-pub-{index}-{uuid.uuid4().hex[:8]}")
        self.client: Optional[mqtt.Client] = None

    def _on_connect(self, client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
        if getattr(reason_code, "is_failure", False) or reason_code_value(reason_code) != 0:
            self.stats.errors.append(f"connect failed: {reason_code}")

    def _on_disconnect(self, client: mqtt.Client, userdata, disconnect_flags, reason_code, properties) -> None:
        if self.stop_event.is_set():
            return
        if reason_code_value(reason_code) != 0:
            self.stats.errors.append(f"disconnect: {reason_code}")

    def run(self) -> None:
        try:
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.stats.client_id,
                protocol=mqtt.MQTTv5,
            )
            self.client = client
            client.enable_logger()
            if self.username is not None:
                client.username_pw_set(self.username, self.password)

            client.on_connect = self._on_connect
            client.on_disconnect = self._on_disconnect

            client.connect(self.host, self.port, keepalive=self.keepalive_seconds)
            client.loop_start()

            deadline = time.time() + self.timeout_seconds
            while not client.is_connected() and time.time() < deadline and not self.stats.errors:
                time.sleep(0.02)

            if not client.is_connected():
                if not self.stats.errors:
                    self.stats.errors.append(f"connect not ready within {self.timeout_seconds}s")
                return

            self.start_event.wait()

            rng = random.Random((self.index + 1) * 1000003)
            run_deadline = time.perf_counter() + self.duration_seconds if self.duration_seconds > 0 else None
            sequence = 0
            next_publish_at = time.perf_counter()
            publish_interval = 1.0 / self.rate_limit_per_second if self.rate_limit_per_second > 0 else 0.0

            while not self.stop_event.is_set():
                if self.messages > 0 and sequence >= self.messages:
                    break
                if run_deadline is not None and time.perf_counter() >= run_deadline:
                    break

                if publish_interval > 0:
                    now = time.perf_counter()
                    if now < next_publish_at:
                        time.sleep(min(next_publish_at - now, 0.01))
                        continue

                topic = self.target_topics[rng.randrange(len(self.target_topics))]
                payload = create_payload(self.payload_size, self.index, sequence)
                info = client.publish(topic, payload=payload, qos=0)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    self.stats.errors.append(f"publish failed: {info.rc}")
                    break

                sequence += 1
                if publish_interval > 0:
                    next_publish_at += publish_interval

            self.stats.sent = sequence
        except Exception as ex:  # noqa: BLE001
            self.stats.errors.append(str(ex))
        finally:
            if self.client is not None:
                try:
                    self.client.disconnect()
                except Exception:  # noqa: BLE001
                    pass
                self.client.loop_stop()


def collect_errors(label: str, items: List[object]) -> List[str]:
    lines: List[str] = []
    for item in items:
        errors = getattr(item, "errors", [])
        if errors:
            lines.append(f"{label} {getattr(item, 'client_id', 'unknown')}: {errors[0]}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description="Independent Python MQTT stress tool using paho-mqtt.")
    parser.add_argument("--host", default="127.0.0.1", help="MQTT broker host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port. Default: 1883")
    parser.add_argument("--topic-prefix", default="stress/target", help="Per-subscriber topic prefix.")
    parser.add_argument("--publishers", type=int, default=8, help="Publisher client count. Default: 8")
    parser.add_argument("--subscribers", type=int, default=8, help="Subscriber client count. Default: 8")
    parser.add_argument("--messages", type=int, default=0, help="Messages per publisher. 0 means use --duration-seconds.")
    parser.add_argument("--duration-seconds", type=float, default=10.0, help="Run duration when --messages=0. Default: 10")
    parser.add_argument("--payload-size", type=int, default=64, help="Payload bytes per publish. Default: 64")
    parser.add_argument("--timeout", type=float, default=15.0, help="Connect/subscribe timeout in seconds. Default: 15")
    parser.add_argument("--settle-seconds", type=float, default=5.0, help="Extra wait after publishers finish. Default: 5")
    parser.add_argument("--keepalive", type=int, default=30, help="MQTT keepalive seconds. Default: 30")
    parser.add_argument("--latency-sample-rate", type=int, default=100, help="Record latency every N received messages. Default: 100")
    parser.add_argument("--rate-limit", type=float, default=1000.0, help="Target total publish rate across all publishers. Default: 1000. 0 means unlimited.")
    parser.add_argument("--username", default=None, help="Optional MQTT username.")
    parser.add_argument("--password", default=None, help="Optional MQTT password.")
    args = parser.parse_args()

    if args.publishers < 1 or args.subscribers < 1:
        raise SystemExit("publishers and subscribers must be >= 1")
    if args.messages < 0 or args.duration_seconds < 0:
        raise SystemExit("messages and duration-seconds must be >= 0")
    if args.messages == 0 and args.duration_seconds == 0:
        raise SystemExit("either set --messages > 0 or --duration-seconds > 0")
    if args.rate_limit < 0:
        raise SystemExit("rate-limit must be >= 0")

    target_topics = [f"{args.topic_prefix}/{index + 1}" for index in range(args.subscribers)]
    per_publisher_rate_limit = (args.rate_limit / args.publishers) if args.rate_limit > 0 else 0.0

    print(f"Target broker       : {args.host}:{args.port}")
    print(f"Topic prefix        : {args.topic_prefix}")
    print(f"Publishers          : {args.publishers}")
    print(f"Subscribers         : {args.subscribers}")
    print(f"Payload bytes       : {max(16, args.payload_size)}")
    print(f"Dependency dir      : {os.environ.get('MQTT_STRESS_PYTHONPATH', DEFAULT_DEPENDENCY_DIR)}")
    if args.rate_limit > 0:
        print(f"Target send rate    : {args.rate_limit:.2f} msg/s total ({per_publisher_rate_limit:.2f} each)")
    if args.messages > 0:
        print(f"Mode                : fixed messages ({args.messages} each)")
    else:
        print(f"Mode                : duration ({args.duration_seconds:.2f}s each)")
    print("")

    ready_events = [threading.Event() for _ in range(args.subscribers)]
    start_event = threading.Event()
    stop_event = threading.Event()

    subscribers = [
        SubscriberWorker(
            index=index,
            host=args.host,
            port=args.port,
            topic=target_topics[index],
            timeout_seconds=args.timeout,
            keepalive_seconds=args.keepalive,
            ready_event=ready_events[index],
            stop_event=stop_event,
            latency_sample_rate=args.latency_sample_rate,
            username=args.username,
            password=args.password,
        )
        for index in range(args.subscribers)
    ]

    publishers = [
        PublisherWorker(
            index=index,
            host=args.host,
            port=args.port,
            timeout_seconds=args.timeout,
            keepalive_seconds=args.keepalive,
            start_event=start_event,
            stop_event=stop_event,
            target_topics=target_topics,
            payload_size=args.payload_size,
            messages=args.messages,
            duration_seconds=args.duration_seconds,
            rate_limit_per_second=per_publisher_rate_limit,
            username=args.username,
            password=args.password,
        )
        for index in range(args.publishers)
    ]

    launch_started = time.perf_counter()
    for worker in subscribers:
        worker.start()

    for event in ready_events:
        event.wait(args.timeout)

    subscriber_errors = collect_errors("subscriber", [worker.stats for worker in subscribers])
    if subscriber_errors:
        for line in subscriber_errors:
            print(f"ERROR: {line}")
        stop_event.set()
        return 2

    for worker in publishers:
        worker.start()

    start_event.set()
    started_at = time.perf_counter()

    for worker in publishers:
        worker.join()

    publish_finished_at = time.perf_counter()
    time.sleep(args.settle_seconds)
    stop_event.set()

    for worker in subscribers:
        worker.join(timeout=2.0)

    finished_at = time.perf_counter()

    publisher_stats = [worker.stats for worker in publishers]
    subscriber_stats = [worker.stats for worker in subscribers]
    total_sent = sum(item.sent for item in publisher_stats)
    total_received = sum(item.received for item in subscriber_stats)
    publish_elapsed = max(0.0001, publish_finished_at - started_at)
    total_elapsed = max(0.0001, finished_at - started_at)

    latencies = [lat for item in subscriber_stats for lat in item.latencies_ms]
    publisher_errors = collect_errors("publisher", publisher_stats)
    subscriber_errors = collect_errors("subscriber", subscriber_stats)

    print("Summary")
    print("-------")
    print(f"Launch + subscribe time : {started_at - launch_started:.3f}s")
    print(f"Publish phase time      : {publish_elapsed:.3f}s")
    print(f"Total observe time      : {total_elapsed:.3f}s")
    print(f"Sent messages           : {total_sent}")
    print(f"Received messages       : {total_received}")
    print(f"Delivery ratio          : {(total_received / total_sent * 100 if total_sent else 0):.2f}%")
    print(f"Send throughput         : {total_sent / publish_elapsed:.2f} msg/s")
    print(f"Receive throughput      : {total_received / total_elapsed:.2f} msg/s")

    if latencies:
        print(f"Latency samples         : {len(latencies)}")
        print(f"Latency avg             : {statistics.mean(latencies):.2f} ms")
        print(f"Latency p50             : {percentile(latencies, 0.50):.2f} ms")
        print(f"Latency p95             : {percentile(latencies, 0.95):.2f} ms")
        print(f"Latency p99             : {percentile(latencies, 0.99):.2f} ms")

    if publisher_errors or subscriber_errors:
        print("")
        print("Errors")
        print("------")
        for line in publisher_errors + subscriber_errors:
            print(line)

    if publisher_errors or subscriber_errors:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
