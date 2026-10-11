"""Local checks for the fixed twenty-minute network observation window."""
import unittest

from phase_timeout_scenarios import NETWORK_DURATION, network_availability_buckets


class NetworkAvailabilityTest(unittest.TestCase):
    def test_early_echo_successes_do_not_cover_a_late_outage(self):
        started = 1000.0
        samples = [{"at": started + i, "finished_at": started + i + 0.8,
                    "rtt": 0.8, "error": None} for i in range(200)]
        samples += [{"at": started + minute * 60 + 5,
                     "finished_at": started + minute * 60 + 11,
                     "rtt": None, "error": None} for minute in range(4, 20)]
        buckets = network_availability_buckets(samples, [], started, NETWORK_DURATION)
        self.assertEqual(len(buckets), 20)
        self.assertGreaterEqual(sum(bucket["echo_successes"] for bucket in buckets), 200)
        self.assertEqual(buckets[-1]["echo_successes"], 0)
        self.assertFalse(all(bucket["echo_successes"] >= 1 for bucket in buckets))

    def test_each_minute_records_echo_and_client_availability(self):
        started = 1000.0
        samples = [{"at": started + minute * 60 + 5,
                    "finished_at": started + minute * 60 + 5.8,
                    "rtt": 0.8, "error": None} for minute in range(20)]
        samples.append({"at": started + 19 * 60 + 10,
                        "finished_at": started + 19 * 60 + 16,
                        "rtt": None, "error": "OSError('closed')"})
        records = [(started + minute * 60 + 2, minute != 19, 1.0, 0, "t1.test")
                   for minute in range(20)]
        buckets = network_availability_buckets(samples, records, started, NETWORK_DURATION)
        self.assertTrue(all(bucket["echo_successes"] >= 1 for bucket in buckets))
        self.assertEqual(buckets[-1]["echo_failures"], 1)
        self.assertEqual(buckets[-1]["echo_errors"], 1)
        self.assertEqual(buckets[-1]["user_requests"], 1)
        self.assertEqual(buckets[-1]["user_successes"], 0)
        self.assertEqual(buckets[-1]["user_failures"], 1)


if __name__ == "__main__":
    unittest.main()
