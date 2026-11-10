import json
from datetime import datetime, timezone
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from azure.core.exceptions import ResourceExistsError
import requests

import collector

FIXTURE = json.loads((Path(__file__).resolve().parents[1] / "example_response.json").read_text())


class CollectorTests(unittest.TestCase):
    def test_round_trip_and_utc_path(self):
        data = json.loads(json.dumps(FIXTURE))
        state = data["states"][0]
        state[12] = [1, 2]
        state[5] = 100  # Long versus double must survive the heterogeneous union.
        state[14] = "1200"
        self.assertEqual(collector.decode(collector.encode(data)), data)
        self.assertEqual(set(collector.decode(collector.encode(data))), {"time", "states"})
        self.assertEqual(collector.path_for(data["time"]),
                         "opensky/states/year=2026/month=10/day=26/hour=11/1793013825.avro")
        self.assertEqual(collector.path_for(1793062800),
                         "opensky/states/year=2026/month=10/day=27/hour=01/1793062800.avro")
        for states in (None, []):
            self.assertEqual(collector.decode(collector.encode({"time": 0, "states": states}))["states"], states)

    def test_reject_invalid_positions_and_original_quarantine(self):
        for index, value in ((0, 42), (4, True), (8, 0), (12, ["bad"]), (16, None)):
            data = json.loads(json.dumps(FIXTURE))
            data["states"][0][index] = value
            with self.subTest(index=index), self.assertRaises(ValueError):
                collector.encode(data)
        for change in ({"time": True}, {"time": -1}, {"extra": 1}, {"states": [[1]]}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                collector.validate({**FIXTURE, **change})
        service = Mock()
        with patch.object(collector, "fetch", return_value=(b'{"time":true,"states":[]}', "123")), \
             patch.object(collector.Path, "read_text", side_effect=["id", "secret"]), \
             self.assertRaises(ValueError):
            collector.run("account", "bronze", "quarantine", "id", "secret", service=service)
        restricted = service.get_container_client.return_value
        self.assertEqual(restricted.upload_blob.call_args.args[1], b'{"time":true,"states":[]}')
        self.assertFalse(any("opensky/states/year=" in str(call) for call in restricted.upload_blob.call_args_list))

    def test_atomic_publish_and_decoded_collision(self):
        bronze, restricted = Mock(), Mock()
        incoming = json.loads(json.dumps(FIXTURE))
        original = json.dumps(incoming).encode()
        path = collector.path_for(incoming["time"])
        self.assertEqual(collector.publish(bronze, restricted, incoming, original), ("created", path))
        self.assertFalse(bronze.upload_blob.call_args.kwargs["overwrite"])
        bronze.upload_blob.side_effect = ResourceExistsError("already exists")
        bronze.download_blob.return_value.readall.return_value = collector.encode(incoming)
        self.assertEqual(collector.publish(bronze, restricted, incoming, original), ("identical", path))
        restricted.upload_blob.assert_not_called()
        changed = json.loads(json.dumps(incoming))
        changed["states"][0][1] = "different"
        with self.assertRaisesRegex(ValueError, "conflicting"):
            collector.publish(bronze, restricted, changed, json.dumps(changed).encode())
        self.assertEqual(bronze.upload_blob.call_args.args[0], path)
        self.assertEqual(restricted.upload_blob.call_count, 1)
        restricted.upload_blob.side_effect = RuntimeError("quarantine down")
        with self.assertRaisesRegex(RuntimeError, "quarantine down"):
            collector.publish(bronze, restricted, changed, original)
        bronze.download_blob.return_value.readall.side_effect = RuntimeError("storage down")
        with self.assertRaisesRegex(RuntimeError, "storage down"):
            collector.publish(bronze, restricted, incoming, original)
        bronze.upload_blob.side_effect = RuntimeError("upload down")
        with self.assertRaisesRegex(RuntimeError, "upload down"):
            collector.publish(bronze, restricted, incoming, original)

    def test_fetch_fixed_box_budget_and_throttle(self):
        session = Mock()
        session.post.return_value.json.return_value = {"access_token": "private-token"}
        ok = Mock(status_code=200, headers={"X-Rate-Limit-Remaining": "1120"}, content=b"{}")
        throttled = Mock(status_code=429, headers={"X-Rate-Limit-Retry-After-Seconds": "3"})
        throttled.raise_for_status.side_effect = requests.HTTPError("private-token")
        session.get.side_effect = [throttled, ok]
        with patch.object(collector, "datetime") as clock:
            clock.now.return_value = datetime(2026, 10, 26, 11, 24, tzinfo=timezone.utc)
            sleep = Mock()
            self.assertEqual(collector.fetch(session, "id", "secret", 60, now=lambda: 0, sleep=sleep), (b"{}", "1120"))
            sleep.assert_called_once_with(3)
        self.assertEqual(session.get.call_args.kwargs["params"], collector.BOX)
        self.assertEqual(session.get.call_args.kwargs["headers"]["Authorization"], "Bearer private-token")
        session.get.side_effect = [throttled, ok]
        with patch.object(collector, "datetime") as clock:
            clock.now.return_value = datetime(2026, 10, 26, 11, 25, tzinfo=timezone.utc)
            with self.assertRaises(RuntimeError):
                collector.fetch(session, "id", "secret", 60, now=lambda: 0)
            self.assertEqual(session.get.call_count, 3)  # no retry outside reserved slots

    def test_server_timeout_and_deadline(self):
        session = Mock()
        session.post.return_value.json.return_value = {"access_token": "secret-token"}
        ok = Mock(status_code=200, headers={}, content=b"{}")
        for first in (requests.Timeout("secret-token"), Mock(status_code=503, headers={})):
            session.get.reset_mock()
            session.get.side_effect = [first, ok]
            with patch.object(collector, "datetime") as clock:
                clock.now.return_value = datetime(2026, 10, 26, 11, 24, tzinfo=timezone.utc)
                self.assertEqual(collector.fetch(session, "id", "secret", 60, now=lambda: 0, sleep=lambda _: None),
                                 (b"{}", None))
                self.assertEqual(session.get.call_count, 2)
        session.get.reset_mock()
        session.get.side_effect = [Mock(status_code=429, headers={"X-Rate-Limit-Retry-After-Seconds": "100"})]
        with patch.object(collector, "datetime") as clock:
            clock.now.return_value = datetime(2026, 10, 26, 11, 24, tzinfo=timezone.utc)
            with self.assertRaises(RuntimeError):
                collector.fetch(session, "id", "secret", 60, now=lambda: 0, sleep=lambda _: None)
            self.assertEqual(session.get.call_count, 1)

    def test_redacted_logs_and_failure_exit(self):
        with patch.object(collector.Path, "read_text", side_effect=RuntimeError("secret-token")), \
             self.assertLogs("collector") as logs, self.assertRaises(RuntimeError):
            collector.run("account", "bronze", "quarantine", "id", "secret")
        self.assertNotIn("secret-token", "\n".join(logs.output))
        with patch("collector.run", side_effect=RuntimeError("secret-token")), \
             patch("sys.argv", ["collector", "--account", "account"]), \
             self.assertRaises(SystemExit) as exited:
            collector.main()
        self.assertEqual(exited.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
