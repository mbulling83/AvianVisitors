import importlib.util
import os
import sqlite3
import tempfile
import unittest

MODULE_PATH = os.path.join(
    os.path.dirname(__file__), '..', 'avian', 'forwarding', 'webhook-forwarder.py'
)


def load_module():
    spec = importlib.util.spec_from_file_location('webhook_forwarder', MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResp:
    def __init__(self, status):
        self.status_code = status


class FakeSession:
    def __init__(self, status=200):
        self.status = status
        self.calls = []

    def post(self, url, **kw):
        self.calls.append((url, kw))
        return FakeResp(self.status)


class TestModuleLoads(unittest.TestCase):
    def test_exposes_expected_functions(self):
        mod = load_module()
        for name in [
            'read_bookmark', 'write_bookmark', 'select_new_detections',
            'resolve_audio_path', 'build_fields', 'post_detection',
            'is_reachable', 'drain_once', 'load_config', 'main',
        ]:
            self.assertTrue(hasattr(mod, name), f'missing {name}')


class TestBookmark(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()
        self.dir = tempfile.mkdtemp()
        self.state = os.path.join(self.dir, 'state.json')

    def test_missing_file_returns_zero(self):
        self.assertEqual(self.mod.read_bookmark(self.state), 0)

    def test_write_then_read_roundtrip(self):
        self.mod.write_bookmark(self.state, 42)
        self.assertEqual(self.mod.read_bookmark(self.state), 42)

    def test_corrupt_file_returns_zero(self):
        with open(self.state, 'w') as f:
            f.write('not json{')
        self.assertEqual(self.mod.read_bookmark(self.state), 0)


class TestSelect(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()
        self.db = os.path.join(tempfile.mkdtemp(), 'birds.db')
        con = sqlite3.connect(self.db)
        con.execute(
            "CREATE TABLE detections (Date DATE, Time TIME, Sci_Name TEXT, "
            "Com_Name TEXT, Confidence FLOAT, Lat FLOAT, Lon FLOAT, Cutoff FLOAT, "
            "Week INT, Sens FLOAT, Overlap FLOAT, File_Name TEXT)"
        )
        rows = [
            ('2026-06-28', '05:14:22', 'Erithacus rubecula', 'European Robin',
             0.91, 51.4, -0.1, 0.7, 26, 1.25, 0.0, 'robin.wav'),
            ('2026-06-28', '05:15:01', 'Pica pica', 'Eurasian Magpie',
             0.55, 51.4, -0.1, 0.7, 26, 1.25, 0.0, 'magpie.wav'),
            ('2026-06-28', '05:16:00', 'Turdus merula', 'Eurasian Blackbird',
             0.80, 51.4, -0.1, 0.7, 26, 1.25, 0.0, 'blackbird.wav'),
        ]
        con.executemany(
            "INSERT INTO detections VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        con.commit()
        con.close()

    def test_selects_all_after_zero(self):
        out = self.mod.select_new_detections(self.db, after_rowid=0, batch_size=25)
        self.assertEqual([r['rowid'] for r in out], [1, 2, 3])
        self.assertEqual(out[0]['Com_Name'], 'European Robin')

    def test_respects_after_rowid(self):
        out = self.mod.select_new_detections(self.db, after_rowid=2, batch_size=25)
        self.assertEqual([r['rowid'] for r in out], [3])

    def test_respects_batch_size_and_order(self):
        out = self.mod.select_new_detections(self.db, after_rowid=0, batch_size=2)
        self.assertEqual([r['rowid'] for r in out], [1, 2])


class TestAudioAndFields(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()
        self.row = {
            'rowid': 7, 'Date': '2026-06-28', 'Time': '05:14:22',
            'Sci_Name': 'Erithacus rubecula', 'Com_Name': 'European Robin',
            'Confidence': 0.91, 'Lat': 51.4, 'Lon': -0.1, 'File_Name': 'robin.wav',
        }

    def test_resolve_audio_path_spaces_to_underscores(self):
        p = self.mod.resolve_audio_path('/base', self.row)
        self.assertEqual(p, '/base/2026-06-28/European_Robin/robin.wav')

    def test_build_fields_contains_idempotency_key(self):
        f = self.mod.build_fields(self.row)
        self.assertEqual(f['detection_id'], '7')
        self.assertEqual(f['species_common'], 'European Robin')
        self.assertEqual(f['species_sci'], 'Erithacus rubecula')
        self.assertEqual(f['detected_at'], '2026-06-28T05:14:22')
        self.assertEqual(f['confidence'], '0.91')
        self.assertEqual(f['audio_filename'], 'robin.wav')


class TestPost(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()
        self.cfg = {
            'ENDPOINT_URL': 'https://example/hook', 'AUTH_TOKEN': 't',
            'AUTH_HEADER': 'Authorization', 'AUTH_SCHEME': 'Bearer',
            'HTTP_TIMEOUT': 5,
        }
        self.row = {'rowid': 1, 'Date': '2026-06-28', 'Time': '05:14:22',
                    'Sci_Name': 'X', 'Com_Name': 'Y', 'Confidence': 0.9,
                    'Lat': 1, 'Lon': 2, 'File_Name': 'a.wav'}
        self.dir = tempfile.mkdtemp()
        self.audio = os.path.join(self.dir, 'a.wav')

    def test_2xx_returns_true_and_sends_auth_header(self):
        with open(self.audio, 'wb') as f:
            f.write(b'RIFFdata')
        s = FakeSession(200)
        ok = self.mod.post_detection(s, self.cfg, self.row, self.audio)
        self.assertTrue(ok)
        url, kw = s.calls[0]
        self.assertEqual(kw['headers']['Authorization'], 'Bearer t')
        self.assertIn('files', kw)
        self.assertEqual(kw['data']['detection_id'], '1')

    def test_non_2xx_returns_false(self):
        with open(self.audio, 'wb') as f:
            f.write(b'x')
        s = FakeSession(500)
        self.assertFalse(self.mod.post_detection(s, self.cfg, self.row, self.audio))

    def test_missing_audio_sends_metadata_only_flag(self):
        s = FakeSession(200)
        ok = self.mod.post_detection(s, self.cfg, self.row, self.audio)  # no file
        self.assertTrue(ok)
        url, kw = s.calls[0]
        self.assertEqual(kw['data']['audio_missing'], 'true')
        self.assertNotIn('files', kw)

    def test_network_exception_returns_false(self):
        class Boom:
            def post(self, *a, **k):
                raise OSError('down')
        with open(self.audio, 'wb') as f:
            f.write(b'x')
        self.assertFalse(
            self.mod.post_detection(Boom(), self.cfg, self.row, self.audio))


class TestDrain(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()
        d = tempfile.mkdtemp()
        self.db = os.path.join(d, 'birds.db')
        self.state = os.path.join(d, 'state.json')
        self.birdsongs = os.path.join(d, 'By_Date')
        con = sqlite3.connect(self.db)
        con.execute(
            "CREATE TABLE detections (Date,Time,Sci_Name,Com_Name,Confidence,"
            "Lat,Lon,Cutoff,Week,Sens,Overlap,File_Name)")
        con.executemany(
            "INSERT INTO detections VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
                ('2026-06-28', '05:14', 'A', 'Robin', 0.91, 1, 2, 0.7, 26, 1.25,
                 0.0, 'a.wav'),
                ('2026-06-28', '05:15', 'B', 'Magpie', 0.40, 1, 2, 0.7, 26, 1.25,
                 0.0, 'b.wav'),
                ('2026-06-28', '05:16', 'C', 'Crow', 0.80, 1, 2, 0.7, 26, 1.25,
                 0.0, 'c.wav'),
            ])
        con.commit()
        con.close()
        self.cfg = {
            'DB_PATH': self.db, 'STATE_PATH': self.state,
            'BIRDSONGS_DIR': self.birdsongs, 'BATCH_SIZE': 25,
            'MIN_CONFIDENCE': 0.0, 'ENDPOINT_URL': 'u', 'AUTH_TOKEN': 't',
            'AUTH_HEADER': 'Authorization', 'AUTH_SCHEME': 'Bearer',
            'HTTP_TIMEOUT': 5,
        }

    def test_all_forwarded_advances_bookmark_to_last(self):
        s = FakeSession(200)
        n = self.mod.drain_once(self.cfg, s)
        self.assertEqual(n, 3)
        self.assertEqual(self.mod.read_bookmark(self.state), 3)

    def test_failure_midway_stops_and_keeps_bookmark_at_last_success(self):
        class FlakySession:
            def __init__(s):
                s.calls = 0

            def post(s, *a, **k):
                s.calls += 1
                return FakeResp(200 if s.calls == 1 else 500)
        self.mod.drain_once(self.cfg, FlakySession())
        self.assertEqual(self.mod.read_bookmark(self.state), 1)

    def test_min_confidence_skips_without_post_but_advances(self):
        self.cfg['MIN_CONFIDENCE'] = 0.7
        s = FakeSession(200)
        self.mod.drain_once(self.cfg, s)
        self.assertEqual(len(s.calls), 2)
        self.assertEqual(self.mod.read_bookmark(self.state), 3)


class TestReachableAndConfig(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()

    def test_is_reachable_false_for_unroutable_quickly(self):
        self.assertFalse(
            self.mod.is_reachable('https://203.0.113.1/hook', timeout=1))

    def test_load_config_reads_module_constants(self):
        cfg = self.mod.load_config()
        self.assertIn('ENDPOINT_URL', cfg)
        self.assertIn('POLL_SECONDS', cfg)
        self.assertEqual(cfg['AUTH_HEADER'], 'Authorization')


if __name__ == '__main__':
    unittest.main()
