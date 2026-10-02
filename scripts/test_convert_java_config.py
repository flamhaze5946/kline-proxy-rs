"""Configuration behavior from Java defaults and explicitly supported overrides."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from convert_java_config import convert


def config(extra=None):
    raw = {'kline.binance.future.intervalSyncConfigs': {'1h': {'listenSymbolPatterns': ['.*?USDT']}}}
    raw.update(extra or {})
    return convert(raw, Path('/java'), '127.0.0.1:1889', Path('/rust'))


class ConfigConversion(unittest.TestCase):
    def test_java_defaults(self):
        value = config()
        self.assertEqual(value['number_type'], 'bigDecimal')
        self.assertEqual(value['subscriptions'][0]['history_capacity'], 365)
        self.assertEqual(value['rest']['future_refresh_count'], 99)
        self.assertEqual(value['rest']['hour_boundary_guard_before_ms'], 150000)
        self.assertEqual(value['rest']['hour_boundary_guard_after_ms'], 30000)
        self.assertFalse(value['strict_readiness'])

    def test_disabled_market_and_empty_patterns(self):
        self.assertEqual(config({'kline.binance.future.enabled': False})['subscriptions'], [])
        self.assertEqual(config({'kline.binance.future.intervalSyncConfigs': {'1h': {'listenSymbolPatterns': []}}})['subscriptions'], [])
        with self.assertRaisesRegex(ValueError, 'explicit list'):
            config({'kline.binance.future.intervalSyncConfigs': {'1h': {}}})

    def test_background_overrides(self):
        value = config({'funding.publicationGraceMs': 500, 'kline.binance.future.rpcRefreshCount': 123,
                        'kline.binance.spot.rpcRefreshCount': None, 'kline.bulk.finalWaitEnabled': False,
                        'kline.diagnostics.closedBarLatencyEnabled': False})
        self.assertEqual(value['market_api']['funding']['publication_grace_ms'], 500)
        self.assertEqual(value['rest']['future_refresh_count'], 123)
        self.assertIsNone(value['rest']['spot_refresh_count'])
        self.assertEqual(value['final_wait_ms'], 0)
        self.assertFalse(value['closed_bar_latency_enabled'])
        self.assertEqual(config({'kline.bulk.finalWaitMaxMs': -1})['final_wait_ms'], 0)
        self.assertEqual(config({'kline.bulk.finalWaitMaxMs': 50000})['final_wait_ms'], 30000)

    def test_nested_and_dotted_keys_are_order_independent(self):
        entries = [('number', {'type': 'float'}), ('kline', {'binance': {'future': {'enabled': False}}}),
                   ('kline.binance.spot.intervalSyncConfigs', {'1d': {'listenSymbolPatterns': ['BTCUSDT']}})]
        self.assertEqual(config(dict(entries)), config(dict(reversed(entries))))
        self.assertEqual(config(dict(entries))['subscriptions'][0]['market'], 'spot')

    def test_persistence_retains_capacity_and_symbol_overrides(self):
        value = config({'kline.persistence': {'enabled': True, 'rootDir': 'snapshots', 'future': {
            'intervalConfigs': {'1h': {'maxStoreCount': 500, 'symbolMaxStoreCounts': {'BTCUSDT': 100, 'ETHUSDT': 700}}}}}})
        sub = value['subscriptions'][0]
        self.assertEqual(sub['history_capacity'], 500)
        self.assertEqual(sub['symbol_capacities'], {'BTCUSDT': 365, 'ETHUSDT': 700})
        self.assertEqual(value['persistence']['legacy_directory'], '/java/snapshots')
        value = config({'kline.persistence': {'enabled': True, 'dumpIntervalSeconds': 0, 'future': {
            'intervalConfigs': {'1h': {'maxStoreCount': None, 'symbolMaxStoreCounts': {'BTCUSDT': None}}}}}})
        self.assertEqual(value['subscriptions'][0]['history_capacity'], 730)
        self.assertEqual(value['subscriptions'][0]['symbol_capacities'], {})
        self.assertEqual(value['persistence']['interval_seconds'], 1)

    def test_client_and_statistic_overrides(self):
        value = config({'client.binanceComposite.api.rootUrl': 'http://localhost:1',
                        'client.blockChainCenter.api.rootUrl': 'http://localhost:2/',
                        'statistic.binance.atr.period': 21, 'statistic.yama01altCoinIndex.statisticDays': 14})
        self.assertEqual(value['market_api']['cms_url'], 'http://localhost:1')
        self.assertEqual(value['market_api']['statistics']['altcoin_url'], 'http://localhost:2/en/altcoin-season-index')
        self.assertEqual(value['market_api']['statistics']['atr_period'], 21)
        self.assertEqual(value['market_api']['statistics']['days'], 14)

    def test_persistence_whitelist_is_independent_of_retention_and_subscription(self):
        periods = {i: {'minMaintainCount': 10, 'listenSymbolPatterns': ['BTCUSDT']} for i in ['1h', '1d']}
        value = config({'kline.binance.future.intervalSyncConfigs': periods,
                        'kline.binance.spot.intervalSyncConfigs': periods,
                        'kline.persistence': {'enabled': True, 'future': {
                            'intervalConfigs': {'1h': {'maxStoreCount': 30}}}}})
        self.assertEqual(len(value['subscriptions']), 4)
        self.assertEqual(value['persistence']['enabled_intervals'], [{'market': 'future', 'interval': '1h'}])
        self.assertEqual([(r['market'], r['interval']) for r in value['persistence']['retention']], [('future', '1h')])
        # Java's global persistence switch still affects in-memory retention, even for unlisted periods.
        self.assertEqual([s['history_capacity'] for s in value['subscriptions']], [30, 20, 20, 20])
        self.assertEqual(config({'kline.persistence.enabled': True})['persistence']['enabled_intervals'], [])

    def test_every_numeric_mode_is_accepted_by_actual_rust_config(self):
        binary = Path(os.environ.get('KLINE_TEST_BINARY', 'target/debug/kline-proxy')).resolve()
        self.assertTrue(binary.is_file(), 'cargo test --workspace builds the binary first')
        for mode in ['double', 'float', 'string', 'bigDecimal']:
            for disabled in [False, True]:
                value = config({'number.type': mode, 'kline.binance.future.enabled': not disabled})
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / 'config.json'
                    path.write_text(json.dumps(value))
                    result = subprocess.run([str(binary), '--check-config', str(path)], capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr + result.stdout)


if __name__ == '__main__':
    unittest.main()
