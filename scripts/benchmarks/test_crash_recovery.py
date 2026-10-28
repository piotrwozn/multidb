import unittest
from pathlib import Path
import tempfile
import threading
from unittest.mock import Mock, patch

from crash_recovery import assert_restored_state, assert_state, BATCH, restore_command, start, validate_acknowledgement, write_inflight_batch
from domain_common import Http


class RecoveryOracleTests(unittest.TestCase):
    def test_lost_ack_partial_batch_or_payload_mismatch_is_rejected(self):
        first = 1000
        base = [[1, 'one']]
        batch = [[i, f'batch-{i}'] for i in range(first, first + BATCH)]
        assert_state(base, {1: 'one'}, [(first, False)])
        assert_state(base + batch, {1: 'one'}, [(first, True)])
        for state, acknowledged in (([], False), (base + batch[:-1], False), (base, True), ([[1, 'bad']], False)):
            with self.subTest(acknowledged=acknowledged), self.assertRaises(AssertionError):
                assert_state(state, {1: 'one'}, [(first, acknowledged)])

    def test_restore_preserves_even_fully_committed_batches_without_an_ack(self):
        first = 1000
        base = [[1, 'one']]
        batch = [[i, f'batch-{i}'] for i in range(first, first + BATCH)]
        snapshot = assert_state(base + batch, {1: 'one'}, [(first, False)])
        # This is an allowed uncertain *crash* outcome, but not an allowed restore
        # of a source that was subsequently observed to contain the whole batch.
        assert_state(base, {1: 'one'}, [(first, False)])
        with self.assertRaises(AssertionError):
            assert_restored_state(base, snapshot)
        assert_restored_state(list(reversed(base + batch)), snapshot)

    def test_shapes_duplicate_keys_unknown_rows_and_ack_types_are_rejected(self):
        for rows in ([[True, 'one']], [[1, 'one'], [1, 'one']], [[1, 1]], [[2, 'two']]):
            with self.subTest(rows=rows), self.assertRaises(AssertionError):
                assert_state(rows, {1: 'one'}, [])
        for affected in (True, 1.0, 0, None):
            with self.subTest(affected=affected), self.assertRaises(AssertionError):
                validate_acknowledgement({'output': {'affected_rows': affected}}, 1)

    def test_unexpected_readiness_exception_cleans_up_its_own_process_and_log(self):
        process = Mock()
        process.poll.return_value = None
        with tempfile.TemporaryDirectory() as directory, patch('crash_recovery.subprocess.Popen', return_value=process) as launch, \
                patch('crash_recovery.Http'), patch('crash_recovery.rows', side_effect=ValueError('invalid response')), \
                patch('crash_recovery.cleanup_processes') as cleanup:
            with self.assertRaises(ValueError):
                start(Path('/fake/multidb'), Path(directory) / 'source.redb', Path(directory), [0])
            cleanup.assert_called_once_with(None, process)
            self.assertTrue(launch.call_args.kwargs['stdout'].closed)
            self.assertTrue(launch.call_args.kwargs['start_new_session'])

    def test_sent_marker_follows_request_transmission_and_precedes_response(self):
        events = []
        http = Http()
        response = Mock(status=200)
        response.read.return_value = b'{"ok":true,"data":{}}'
        http.connection = Mock()
        http.connection.request.side_effect = lambda *args: events.append('request')
        http.connection.getresponse.side_effect = lambda: events.append('response') or response
        http.json('POST', '/sql', {'sql': 'select 1'}, on_sent=lambda: events.append('sent'))
        self.assertEqual(events, ['request', 'sent', 'response'])
        events.clear()
        http.connection.request.side_effect = OSError('not transmitted')
        with self.assertRaises(OSError):
            http.json('POST', '/sql', {}, on_sent=lambda: events.append('sent'))
        self.assertEqual(events, [])

    def test_sql_errors_are_not_mistaken_for_a_crash_induced_unknown_outcome(self):
        with patch('crash_recovery.Http') as constructor:
            constructor.return_value.__enter__.return_value.json.side_effect = RuntimeError('HTTP500 invalid SQL')
            result = {}
            write_inflight_batch(1000, threading.Event(), result)
            self.assertIn('unexpected_failure', result)
            self.assertNotIn('acknowledged', result)

    def test_restore_selects_the_actual_fixture_profile_for_an_absent_target(self):
        command = restore_command(Path('/fake/multidb'), Path('/tmp/backup'), Path('/tmp/new.redb'))
        self.assertEqual(command[command.index('--profile') + 1], 'transactional')
        self.assertNotIn('auto', command)
