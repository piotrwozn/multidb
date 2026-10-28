#!/usr/bin/env python3
"""Disposable acknowledged-write/process-kill/atomic-batch and restore drill.

This tests abrupt process death, not power loss or hardware/storage certification.
It starts no remote service and never opens an existing operator database.
"""
from __future__ import annotations

import argparse
from http.client import HTTPException
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time

from compare import cleanup_processes, cpu_command, require_free_ports, server_environment, sha256, write_json
from domain_common import Http

TABLE = 'crash_probe'
BATCH = 100


def rows(http, sql):
    return http.json('POST', '/sql', {'sql': sql})['output']['rows']


def start(binary, database, directory, cpu_set):
    log = (directory / 'server.log').open('ab')
    process = None
    try:
        process = subprocess.Popen(cpu_command(cpu_set, [binary, 'serve']), env=server_environment(database),
                                   stdout=log, stderr=subprocess.STDOUT, cwd=directory, start_new_session=True)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f'server exited at startup:{process.returncode}')
            try:
                with Http() as http:
                    http.connection.timeout = min(2, max(.1, deadline - time.monotonic()))
                    if rows(http, 'SELECT 1') == [[1]]:
                        return process, log
            except (OSError, RuntimeError, KeyError):
                time.sleep(.1)
        raise TimeoutError('server did not become ready')
    except BaseException:
        try:
            if process is not None:
                cleanup_processes(None, process)
        finally:
            log.close()
        raise


def logical_state(actual):
    if any(not isinstance(row, list) or len(row) != 2 or type(row[0]) is not int or type(row[1]) is not str for row in actual):
        raise AssertionError('invalid recovered row shape')
    state = dict(actual)
    if len(state) != len(actual):
        raise AssertionError('duplicate recovered primary keys')
    return state


def assert_state(actual, confirmed, batches):
    state = logical_state(actual)
    for identifier, payload in confirmed.items():
        if state.get(identifier) != payload:
            raise AssertionError(f'acknowledged row lost or changed:{identifier}')
    known = set(confirmed)
    for first, acknowledged in batches:
        identifiers = set(range(first, first + BATCH))
        present = identifiers.intersection(state)
        if len(present) not in (0, BATCH) or (acknowledged and len(present) != BATCH):
            raise AssertionError(f'crashed atomic batch is partial or lost:{first}, count={len(present)}')
        for identifier in present:
            if state[identifier] != f'batch-{identifier}':
                raise AssertionError('batch payload changed')
        known.update(identifiers)
    if set(state) - known:
        raise AssertionError('unexpected recovered rows')
    return state


def assert_restored_state(actual, source_snapshot):
    if logical_state(actual) != source_snapshot:
        raise AssertionError('restored logical rows differ from the complete source snapshot')


def validate_acknowledgement(response, expected):
    affected = response['output'].get('affected_rows')
    if type(affected) is not int or affected != expected:
        raise AssertionError('write returned an invalid successful acknowledgement')


def restore_command(binary, backup, restored):
    # The source fixture is explicitly Transactional; an absent restore target
    # cannot use auto, which only probes an already existing database.
    return [str(binary), 'restore', '--backup', str(backup), '--dest-db', str(restored), '--profile', 'transactional']


def write_inflight_batch(first, sent, result):
    try:
        with Http() as writer:
            command = f'INSERT INTO {TABLE} VALUES ' + ','.join(f"({i},'batch-{i}')" for i in range(first, first + BATCH))
            response = writer.json('POST', '/sql', {'sql': command}, on_sent=sent.set)
            validate_acknowledgement(response, BATCH)
            result['acknowledged'] = True
    except Exception as failure:
        result['failure'] = type(failure).__name__
        if not isinstance(failure, (OSError, HTTPException, json.JSONDecodeError)):
            result['unexpected_failure'] = str(failure)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cycles', type=int, default=20)
    parser.add_argument('--writes-per-cycle', type=int, default=50)
    args = parser.parse_args()
    binary, output = args.binary.resolve(), args.output.resolve()
    if output.exists() or not binary.is_file() or min(args.cycles, args.writes_per_cycle) < 1:
        raise ValueError('fresh output, an existing binary and positive counts are required')
    require_free_ports()
    output.mkdir(parents=True)
    database = output / 'source.redb'
    cpu_set = sorted(os.sched_getaffinity(0))[:2]
    write_json(output / 'run.json', {'binary': str(binary), 'sha256': sha256(binary),
                                   'cycles': args.cycles, 'writes_per_cycle': args.writes_per_cycle,
                                   'fault': 'SIGKILL after transmitting a 100-row atomic batch request; commit may precede kill',
                                   'scope': 'process death, immediate durability, local filesystem; not power loss'})
    process, log = None, None
    confirmed, batches, outcomes = {}, [], []
    try:
        process, log = start(binary, database, output, cpu_set)
        with Http() as http:
            http.json('POST', '/builder/table', {'name': TABLE, 'schema': {'primary_key': 0, 'columns': [
                {'name': 'id', 'ty': 'Int', 'nullable': False}, {'name': 'payload', 'ty': 'Str', 'nullable': False},
            ]}, 'indexes': []})
        for cycle in range(args.cycles):
            with Http() as http:
                for index in range(args.writes_per_cycle):
                    identifier = cycle * args.writes_per_cycle + index
                    payload = f'confirmed-{identifier}'
                    response = http.json('POST', '/sql', {'sql': f"INSERT INTO {TABLE} VALUES ({identifier},'{payload}')"})
                    validate_acknowledgement(response, 1)
                    confirmed[identifier] = payload
            first = 1_000_000 + cycle * BATCH
            sent = threading.Event()
            result = {}
            thread = threading.Thread(target=write_inflight_batch, args=(first, sent, result), daemon=True)
            thread.start()
            if not sent.wait(5):
                raise TimeoutError('in-flight writer did not start')
            delay = .001 * (cycle % 3)
            time.sleep(delay)
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
            log.close()
            log = None
            thread.join(timeout=10)
            if thread.is_alive():
                raise TimeoutError('writer did not observe process death')
            if 'unexpected_failure' in result:
                raise AssertionError(f'atomic batch failed independently of process death:{result}')
            acknowledged = result.get('acknowledged', False)
            batches.append((first, acknowledged))
            process, log = start(binary, database, output, cpu_set)
            with Http() as http:
                recovered = rows(http, f'SELECT id,payload FROM {TABLE} ORDER BY id')
            source_snapshot = assert_state(recovered, confirmed, batches)
            outcomes.append({'cycle': cycle + 1, 'confirmed_singles': len(confirmed), 'recovered_rows': len(recovered),
                             'inflight_batch_acknowledged': acknowledged, 'request_transmitted': sent.is_set(),
                             'delay_after_send_ms': delay * 1000, 'writer_result': result, 'passed': True})
            write_json(output / 'cycles.json', outcomes)
        shutdown = cleanup_processes(None, process)
        if process.returncode or shutdown.get('forced_kill'):
            raise RuntimeError('final source shutdown was not clean')
        process = None
        if log:
            log.close()
            log = None
        backups = output / 'backups'
        for command in ([binary, 'backup', 'full', '--db', database, '--profile', 'auto', '--dest', backups],):
            with (output / 'backup.log').open('ab') as stream:
                subprocess.run(list(map(str, command)), check=True, stdout=stream, stderr=subprocess.STDOUT, timeout=120,
                               env=server_environment(database))
        candidates = [path for path in backups.iterdir() if path.is_dir()]
        if len(candidates) != 1:
            raise AssertionError('expected one new full backup')
        restored = output / 'restored.redb'
        with (output / 'verify-restore.log').open('wb') as stream:
            subprocess.run([str(binary), 'verify', '--backup', str(candidates[0])], check=True, stdout=stream, stderr=subprocess.STDOUT, timeout=120,
                           env=server_environment(database))
            subprocess.run(restore_command(binary, candidates[0], restored), check=True, stdout=stream, stderr=subprocess.STDOUT, timeout=120,
                           env=server_environment(database))
        process, log = start(binary, restored, output, cpu_set)
        with Http() as http:
            restored_rows = rows(http, f'SELECT id,payload FROM {TABLE} ORDER BY id')
            assert_state(restored_rows, confirmed, batches)
            assert_restored_state(restored_rows, source_snapshot)
        restored_shutdown = cleanup_processes(None, process)
        if process.returncode or restored_shutdown.get('forced_kill'):
            raise RuntimeError('restored database shutdown was not clean')
        process = None
        if log:
            log.close()
            log = None
        write_json(output / 'result.json', {'passed': True, 'cycles': len(outcomes), 'confirmed_singles': len(confirmed),
                                          'atomic_batches_checked': len(batches), 'full_backup_verified_and_restored': True,
                                          'inflight_batches_without_ack': sum(not ack for _, ack in batches),
                                          'source_sha256': sha256(binary)})
    except BaseException as failure:
        write_json(output / 'failure.json', {'type': type(failure).__name__, 'message': str(failure)})
        raise
    finally:
        if process:
            cleanup_processes(None, process)
        if log:
            log.close()


if __name__ == '__main__':
    main()
