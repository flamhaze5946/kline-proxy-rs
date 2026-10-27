#!/usr/bin/env python3
"""Offline unit tests for the rehearsal kit (no sockets): python3 -m unittest rehearsal/test_kit.py"""
import asyncio
import copy
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kp_lib as L  # noqa: E402
import recorder_evidence as R  # noqa: E402
import repull as RP  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "crates", "kline-service", "tests", "fixtures", "kp_evidence_v1_example.json")


class Invariant(unittest.TestCase):
    def setUp(self):
        with open(FIXTURE) as f:
            self.d = json.load(f)
        self.req = sorted(self.d["status"])

    def test_pinned_fixture_is_clean(self):
        self.assertEqual(L.window_violations(self.d, requested=self.req, limit=5), [])

    def test_negative_cases_are_detected(self):
        cases = []
        d = copy.deepcopy(self.d)  # a real row deleted and reported absent twice -> overlap
        d["absent"]["BTCUSDT"].append(d["klines"]["BTCUSDT"][0][0])
        cases.append(d)
        d = copy.deepcopy(self.d)  # a minute dropped from the tiling
        d["klines"]["BTCUSDT"].pop(1)
        d["evidence"]["BTCUSDT"].pop(1)
        cases.append(d)
        d = copy.deepcopy(self.d)  # synthetic row reported final
        d["evidence"]["BNBUSDT"][2] = "final"
        cases.append(d)
        d = copy.deepcopy(self.d)  # a symbol removed from status
        del d["status"]["ETHUSDT"]
        cases.append(d)
        d = copy.deepcopy(self.d)  # evidence length mismatch
        d["evidence"]["BTCUSDT"].append("final")
        cases.append(d)
        for d in cases:
            self.assertTrue(L.window_violations(d, requested=self.req, limit=5), d)

    def test_minute_state_and_bridge(self):
        d = self.d
        self.assertEqual(L.minute_state(d, "ETHUSDT", 120000), "absent")
        self.assertEqual(L.minute_state(d, "BNBUSDT", 120000), "pending")
        self.assertEqual(L.minute_state(d, "UNKNOWNUSDT", 0), "unknown")
        self.assertTrue(L.bridge_complete(d, "ETHUSDT", 0, 240000))
        self.assertFalse(L.bridge_complete(d, "BNBUSDT", 0, 240000))


class Frames(unittest.TestCase):
    def roundtrip(self, payload, mask):
        raw = L.ws_encode(L.OP_TEXT, payload, mask=mask)

        async def go():
            r = asyncio.StreamReader()
            r.feed_data(raw)
            r.feed_eof()
            return await L.ws_read_frame(r)
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(go()), raw
        finally:
            loop.close()

    def test_masked_and_long_frames(self):
        for n in (0, 10, 125, 126, 70000):
            for mask in (False, True):
                p = (b"x" * n)
                (fin, op, payload, raw2), raw = self.roundtrip(p, mask)
                self.assertTrue(fin)
                self.assertEqual((op, payload, raw2), (L.OP_TEXT, p, raw))

    def test_topic_parsing(self):
        p = b'{"stream":"btcusdt_perpetual@continuousKline_1m","data":{"k":{"t":1,"x":true}}}'
        self.assertEqual(L.frame_topic(p), "btcusdt_perpetual@continuousKline_1m")
        self.assertEqual(L.topic_symbol(L.frame_topic(p)), "BTCUSDT")
        self.assertEqual(L.symbol_topic("BTCUSDT"), "btcusdt_perpetual@continuousKline_1m")
        self.assertIsNone(L.frame_topic(b'{"result":null,"id":1}'))

    def test_accept_key_rfc_example(self):
        self.assertEqual(L.ws_accept("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")


class Budget(unittest.TestCase):
    def test_rolling_cap(self):
        b = L.RollingBudget(10)
        self.assertTrue(b.try_spend(6, t=0))
        self.assertFalse(b.try_spend(5, t=1000))
        self.assertTrue(b.try_spend(4, t=1000))
        self.assertTrue(b.try_spend(6, t=60_001))  # the first spend aged out

    def test_weights(self):
        self.assertEqual([L.kline_weight(n) for n in (3, 99, 100, 241, 499, 500, 1000, 1500)], [1, 1, 2, 2, 2, 5, 5, 10])

    def test_recorder_budget_assertions(self):
        a = R.parse_args(["--hours", "1", "--out", "/nonexistent"])
        self.assertEqual(R.check_budget(a, 4, 3), 4 * 3 * 4 + 1)
        with self.assertRaises(AssertionError):
            R.check_budget(a, 20, 3)  # 241 > recorder cap 120
        a = R.parse_args(["--hours", "1", "--out", "/x", "--other-weight", "600"])
        with self.assertRaises(AssertionError):
            R.check_budget(a, 4, 3)  # 1200 + 600 + 120 > 1800
        a = R.parse_args(["--hours", "1", "--out", "/x", "--ip-budget", "2400", "--other-weight", "600"])
        with self.assertRaises(AssertionError):
            R.check_budget(a, 4, 3)  # budget above the design ceiling


class SchemaV1(unittest.TestCase):
    """P3-S3a schema v1 (fix2 > fix1 > section 2) shapes produced by the kit."""

    def test_bulk_record_has_exactly_one_body_field(self):
        ok = R.bulk_record(60_000, 0.5, 3, ["A", "B"], 60_500, 0.1, 200, b'{"x":1}', None)
        self.assertEqual(ok["resp_text"], '{"x":1}')
        self.assertNotIn("error", ok)
        self.assertNotIn("resp", ok)
        self.assertEqual((ok["schema"], ok["kind"], ok["n_req"], ok["with_evidence"]), (1, "bulk", 2, True))
        bad = R.bulk_record(60_000, 0.5, 3, ["A"], 60_500, 0.1, 503, b"busy", None)
        self.assertTrue(bad["error"].startswith("HTTP 503"))
        self.assertNotIn("resp_text", bad)
        err = R.bulk_record(60_000, 0.5, 3, ["A"], None, None, None, b"", "timeout")
        self.assertEqual(err["error"], "timeout")
        with self.assertRaises(AssertionError):
            R.bulk_record(60_000, 0.5, 3, [], 60_500, 0.1, 200, b"{}", None)

    def test_repull_runs_and_maturity(self):
        h = 3_600_000
        mins = [h - 120_000, h - 60_000, h, h + 60_000, h + 300_000]
        self.assertEqual(RP.runs(mins), [[h - 120_000, h - 60_000], [h, h + 60_000], [h + 300_000]])
        self.assertEqual(len(RP.runs([i * 60_000 for i in range(50)])[0]), 35)
        self.assertEqual(RP.day_end(5), 86_400_000)
        self.assertEqual(RP.day_end(86_400_000), 2 * 86_400_000)


class Config(unittest.TestCase):
    def test_vps_sample_matches_section6(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "kp-1m.json")) as f:
            c = json.load(f)
        p = c["persistence"]
        self.assertEqual((p["boundary_guard_before_ms"], p["boundary_guard_after_ms"]), (2000, 15000))
        self.assertEqual((c["rest"]["boundary_guard_before_ms"], c["rest"]["boundary_guard_after_ms"]), (3000, 10000))
        self.assertIn("latest_repair_workers", c["rest"])
        self.assertGreaterEqual(c["evidence"]["ws_trust_window_ms"], 60_000)
        self.assertEqual(c["evidence"]["ws_trust_window_ms"], 1440 * 60_000)
        self.assertFalse(c["market_api"]["enabled"])
        self.assertFalse(c["market_api"]["funding"]["vision_enabled"])
        self.assertFalse(c["market_api"]["statistics"]["enabled"])
        self.assertEqual(c["subscriptions"][0]["history_capacity"], 241)
        self.assertTrue(c["subscriptions"][0]["continuous"])
        self.assertEqual(c["number_type"], "string")
        self.assertGreater(c["final_wait_ms"], 0)
        for k in ("bulk_admission_wait_ms", "http_admission_wait_ms", "bulk_pre_boundary_wait_ms"):
            self.assertIn(k, c)
        self.assertTrue(c["rest"]["future_url"].startswith("http://127.0.0.1:"))
        self.assertTrue(c["websocket"]["future_url"].startswith("ws://127.0.0.1:"))

    def test_unit_memory_max(self):
        unit = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "systemd", "kp-1m-evidence.service")).read()
        self.assertIn("MemoryMax=1.4G", unit)


if __name__ == "__main__":
    unittest.main()
