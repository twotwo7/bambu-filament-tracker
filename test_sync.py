import copy
import unittest

import sync


class FakeCloud:
    def __init__(self, spools):
        self.data = copy.deepcopy(spools)
        self.calls = []
        self.fail_after_write = False

    def spools(self):
        return copy.deepcopy(self.data)

    def update_weight(self, spool, grams):
        self.calls.append((str(spool["id"]), grams))
        item = next(x for x in self.data if str(x["id"]) == str(spool["id"]))
        item["netWeight"] = grams
        if self.fail_after_write:
            self.fail_after_write = False
            raise sync.SyncError("simulated timeout after remote write")


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.cfg = {
            "spools": [
                {"id": "1", "label": "beige", "material": "PETG", "initial_grams": 1000,
                 "historical_target_colors": ["112233FF"]},
                {"id": "2", "label": "clear", "material": "PETG", "initial_grams": 1000,
                 "historical_filament_ids": ["DEMO01"]},
            ],
            "manual_estimates": [{"job_id": "cancel", "spool_id": "1", "fraction": "0.8", "reason": "user estimate"}],
        }
        self.spools = [
            {"id": 1, "filamentName": "PETG Basic", "filamentType": "PETG", "color": "#445566FF", "netWeight": 480, "inPrinter": True, "amsId": 0, "slotId": "3"},
            {"id": 2, "filamentName": "PETG Translucent", "filamentType": "PETG", "color": "#AABBCC00", "netWeight": 900, "inPrinter": True, "amsId": 0, "slotId": "1"},
        ]
        self.con = sync.connect(":memory:")

    def tearDown(self):
        self.con.close()

    @staticmethod
    def job(ident, status, weight, color, slot=3, fid=""):
        return {"id": ident, "title": ident, "status": status, "startTime": "2025-01-01T00:00:00Z",
                "amsDetailMapping": [{"filamentType": "PETG", "targetFilamentType": "PETG",
                                      "filamentId": fid, "targetColor": color, "weight": weight,
                                      "amsId": 0, "slotId": slot}]}

    def test_user_cancel_estimate_and_other_failure(self):
        jobs = [self.job("done", 2, 100.5, "112233FF"),
                self.job("cancel", 3, 51.0, "112233FF"),
                self.job("unknown", 3, 10, "112233FF")]
        sync.initialize(self.con, self.cfg, self.spools, jobs)
        spool = self.con.execute("SELECT * FROM spools WHERE id='1'").fetchone()
        goal, used = sync.desired(self.con, spool)
        self.assertEqual((goal, used), (859, 141300))
        states = {r["id"]: r["state"] for r in self.con.execute("SELECT * FROM jobs")}
        self.assertEqual(states, {"done": "applied", "cancel": "applied", "unknown": "review"})

    def test_running_job_is_deducted_once_after_completion(self):
        task = self.job("active", 4, 72.50, "AABBCC00", slot=1)
        sync.initialize(self.con, self.cfg, self.spools, [task])
        cloud = FakeCloud(self.spools)
        self.assertEqual(sync.push(self.con, cloud), [])
        task["status"] = 2
        sync.ingest(self.con, self.cfg, cloud.spools(), [task])
        self.assertEqual(sync.push(self.con, cloud), [])
        self.assertEqual(cloud.calls[-1], ("2", 928))
        before = list(cloud.calls)
        sync.ingest(self.con, self.cfg, cloud.spools(), [task])
        self.assertEqual(sync.push(self.con, cloud), [])
        self.assertEqual(cloud.calls, before)

    def test_timeout_after_write_recovers_without_second_deduction(self):
        jobs = [self.job("done", 2, 100, "112233FF")]
        sync.initialize(self.con, self.cfg, self.spools, jobs)
        cloud = FakeCloud(self.spools)
        cloud.fail_after_write = True
        self.assertTrue(sync.push(self.con, cloud))
        self.assertEqual(cloud.calls[0], ("1", 900))
        self.assertEqual(sync.push(self.con, cloud), [])
        self.assertEqual(cloud.calls.count(("1", 900)), 1)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM operations WHERE status='pending'").fetchone()[0], 0)

    def test_external_edit_does_not_get_overwritten(self):
        sync.initialize(self.con, self.cfg, self.spools, [])
        cloud = FakeCloud(self.spools)
        self.assertEqual(sync.push(self.con, cloud), [])
        cloud.data[0]["netWeight"] = 300
        warnings = sync.push(self.con, cloud)
        self.assertTrue(any("300" in warning for warning in warnings))
        self.assertEqual(cloud.data[0]["netWeight"], 300)
        sync.accept_cloud_weight(self.con, cloud, "1")
        self.assertEqual(sync.push(self.con, cloud), [])
        self.assertEqual(cloud.data[0]["netWeight"], 300)
        self.assertEqual(self.con.execute("SELECT accepted_cloud_g FROM calibrations").fetchone()[0], 300)

    def test_cloud_terminal_status_change_is_flagged_without_a_second_debit(self):
        task = self.job("done", 2, 10, "112233FF")
        sync.initialize(self.con, self.cfg, self.spools, [task])
        task["status"] = 3
        sync.ingest(self.con, self.cfg, self.spools, [task])
        self.assertEqual(self.con.execute("SELECT state FROM jobs WHERE id='done'").fetchone()[0], "review_changed")
        self.assertEqual(self.con.execute("SELECT SUM(amount_mg) FROM usage").fetchone()[0], 10000)

    def test_duplicate_color_and_unbound_spool_are_not_guessed(self):
        task = self.job("new", 2, 10, "445566FF")
        duplicate = copy.deepcopy(self.spools[0])
        duplicate["id"] = 3
        self.assertIsNone(sync.current_match(task["amsDetailMapping"][0], self.cfg, self.spools + [duplicate]))
        unbound = copy.deepcopy(self.spools)
        unbound[0]["inPrinter"] = False
        self.assertIsNone(sync.current_match(task["amsDetailMapping"][0], self.cfg, unbound))
        missing_target_type = copy.deepcopy(task["amsDetailMapping"][0])
        missing_target_type["targetFilamentType"] = ""
        self.assertIsNone(sync.current_match(missing_target_type, self.cfg, self.spools))
        sync.initialize(self.con, self.cfg, self.spools, [])
        sync.ingest(self.con, self.cfg, unbound, [task])
        self.assertEqual(self.con.execute("SELECT state FROM jobs WHERE id='new'").fetchone()[0], "review")
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
