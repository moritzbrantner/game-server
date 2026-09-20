import copy
import unittest

from run import compare


def receipt(median=1000):
    return {
        "schemaVersion": 1,
        "environment": {"cpu": "fixture", "profile": "release"},
        "harnessSha256": "fixture",
        "scenarios": [{
            "name": "fixture_workload",
            "unit": "ns/op",
            "direction": "lower",
            "iterationsPerSample": 100,
            "warmupBatches": 5,
            "median": median,
            "regressionLimit": {"relative": 0.20, "absoluteNs": 25},
        }],
    }


class ComparisonTests(unittest.TestCase):
    def test_only_regressions_above_relative_and_absolute_noise_limits_fail(self):
        for before, after, expected in [(1000, 1300, True), (1000, 1199, False), (10, 20, False), (1000, 700, False)]:
            with self.subTest(before=before, after=after):
                self.assertEqual(compare(receipt(before), receipt(after))[0]["regressed"], expected)

    def test_different_environment_or_harness_cannot_claim_a_comparison(self):
        baseline = receipt()
        for key in ["environment", "harnessSha256", "schemaVersion"]:
            candidate = copy.deepcopy(baseline)
            candidate[key] = "changed"
            with self.subTest(key=key), self.assertRaises(ValueError):
                compare(baseline, candidate)

    def test_missing_or_differently_sampled_scenarios_fail_closed(self):
        baseline = receipt()
        missing = receipt()
        missing["scenarios"] = []
        with self.assertRaises(ValueError):
            compare(baseline, missing)
        for key in ["unit", "direction", "iterationsPerSample", "warmupBatches"]:
            candidate = receipt()
            candidate["scenarios"][0][key] = "changed"
            with self.subTest(key=key), self.assertRaises(ValueError):
                compare(baseline, candidate)

    def test_duplicate_scenarios_and_invalid_timings_cannot_pass(self):
        baseline = receipt()
        duplicate = receipt()
        duplicate["scenarios"].append(copy.deepcopy(duplicate["scenarios"][0]))
        with self.assertRaises(ValueError):
            compare(baseline, duplicate)
        for median in [0, -1, float("nan"), float("inf")]:
            with self.subTest(median=median), self.assertRaises(ValueError):
                compare(baseline, receipt(median))
