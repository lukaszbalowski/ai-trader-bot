import unittest

import pandas as pd

from execution_analysis import ReplayTrade, build_recommendations, classify_recommendation, pair_replays_to_live


class ExecutionAnalysisTests(unittest.TestCase):
    def test_pair_replays_to_live_matches_by_market_and_strategy(self):
        replay = ReplayTrade(
            market_id="m1",
            timeframe="BTC_5m",
            symbol="BTC",
            strategy_key="momentum",
            direction="UP",
            entry_idx=1,
            entry_time=pd.Timestamp("2026-02-26T21:00:05"),
            entry_price=0.42,
            exit_idx=3,
            exit_time=pd.Timestamp("2026-02-26T21:00:15"),
            exit_price=0.63,
            return_pct=50.0,
            pnl_usd=0.5,
            exit_reason="replay_global_tp_200",
            session_id="sess",
            config_source="tracked",
        )
        live_df = pd.DataFrame(
            [
                {
                    "trade_id": "t1",
                    "market_id": "m1",
                    "strategy_key": "momentum",
                    "direction": "UP",
                    "entry_price": 0.44,
                    "entry_return_pct": 40.0,
                    "entry_dt": pd.Timestamp("2026-02-26T21:00:06"),
                }
            ]
        )

        matched, unmatched_replays, unmatched_live = pair_replays_to_live([replay], live_df)

        self.assertEqual(len(matched), 1)
        self.assertEqual(len(unmatched_replays), 0)
        self.assertEqual(len(unmatched_live), 0)
        self.assertEqual(matched[0][0].market_id, "m1")
        self.assertEqual(matched[0][1]["trade_id"], "t1")

    def test_classify_recommendation_prioritizes_entry_filter_review(self):
        row = pd.Series(
            {
                "replay_trades": 4,
                "live_trades": 0,
                "live_total_pnl": 0.0,
                "replay_total_pnl": 1.2,
                "avg_alt_exit_improvement_pp": 2.0,
                "avg_resolution_improvement_pp": 1.0,
            }
        )
        self.assertEqual(classify_recommendation(row), "review_entry_filters")

    def test_build_recommendations_combines_missed_and_exit_data(self):
        replay_summary = pd.DataFrame(
            [
                {
                    "timeframe": "BTC_5m",
                    "strategy_key": "momentum",
                    "replay_trades": 2,
                    "replay_total_pnl": 1.5,
                    "replay_avg_return_pct": 15.0,
                    "replay_win_rate": 50.0,
                }
            ]
        )
        live_summary = pd.DataFrame(
            [
                {
                    "timeframe": "BTC_5m",
                    "strategy_key": "momentum",
                    "live_trades": 1,
                    "live_total_pnl": -0.2,
                    "live_avg_return_pct": -20.0,
                    "live_win_rate": 0.0,
                }
            ]
        )
        missed_signals = [
            ReplayTrade(
                market_id="m2",
                timeframe="BTC_5m",
                symbol="BTC",
                strategy_key="momentum",
                direction="DOWN",
                entry_idx=2,
                entry_time=pd.Timestamp("2026-02-26T21:02:00"),
                entry_price=0.33,
                exit_idx=4,
                exit_time=pd.Timestamp("2026-02-26T21:02:20"),
                exit_price=0.52,
                return_pct=57.57,
                pnl_usd=0.57,
                exit_reason="replay_secure_before_expiry",
                session_id="sess",
                config_source="tracked",
            )
        ]
        alt_exit_df = pd.DataFrame(
            [
                {
                    "trade_id": "t1",
                    "strategy_key": "momentum",
                    "best_simple_rule": "take_profit_30pct",
                    "best_simple_improvement_pp": 18.0,
                }
            ]
        )
        hold_df = pd.DataFrame(
            [
                {
                    "trade_id": "t1",
                    "strategy_key": "momentum",
                    "resolution_improvement_pp": 12.0,
                }
            ]
        )
        recommendations = build_recommendations(
            replay_summary,
            live_summary,
            missed_signals,
            alt_exit_df,
            hold_df,
            {"BTC_5m": {"momentum": {"_pnl_percent": 123.4}}},
        )

        self.assertEqual(len(recommendations), 1)
        row = recommendations.iloc[0]
        self.assertEqual(int(row["missed_signals"]), 1)
        self.assertEqual(row["top_exit_rule"], "take_profit_30pct")
        self.assertEqual(row["recommendation"], "execution_drift")
        self.assertAlmostEqual(float(row["vault_best_pnl_percent"]), 123.4)


if __name__ == "__main__":
    unittest.main()
