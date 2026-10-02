# -*- coding: utf-8 -*-
"""Python 2.6+ standard-library archive -> bounded TSV staging (one site)."""
from __future__ import with_statement
import calendar
import datetime
import hashlib
import json
import os
import re
import subprocess
import xml.etree.ElementTree as ET

TABLES = {
    'posts': ['Id', 'PostTypeId', 'CreationDate', 'ParentId', 'month'],
    'users': ['Id', 'CreationDate', 'month'],
    'votes': ['Id', 'VoteTypeId', 'CreationDate', 'month']}


def read_json(path):
    with open(path, 'rb') as stream:
        return json.loads(stream.read().decode('utf-8'))


def digest(path, algorithm='sha256'):
    result = hashlib.new(algorithm)
    with open(path, 'rb') as stream:
        while True:
            block = stream.read(1024 * 1024)
            if not block:
                break
            result.update(block)
    return result.hexdigest()


def write_json(path, value):
    with open(path + '.tmp', 'wb') as stream:
        stream.write((json.dumps(value, sort_keys=True, indent=2) + '\n').encode('utf-8'))
    os.rename(path + '.tmp', path)


def epoch(text):
    if not re.match(r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z?$', text):
        raise ValueError('Invalid UTC timestamp: ' + text)
    dt = datetime.datetime.strptime(text[:19], '%Y-%m-%dT%H:%M:%S')
    return calendar.timegm(dt.timetuple()), dt.year, dt.month


def stream_table(stream, table, dest, chunk_rows=100000, reserve_bytes=1024 ** 3):
    """One open output file per year; no XML tree or row batch retained."""
    if chunk_rows < 1:
        raise ValueError('chunk_rows must be positive')
    handles, counts, parts = {}, {}, {}
    first = None
    try:
        context = iter(ET.iterparse(stream, events=('start', 'end')))
        event, root = next(context)
        if event != 'start' or root.tag.lower() != table:
            raise ValueError('Wrong XML root for ' + table)
        for event, elem in context:
            if event != 'end' or elem.tag != 'row':
                continue
            a = elem.attrib
            seconds, year, month = epoch(a['CreationDate'])
            if a.get('Id') is None:
                raise ValueError('Missing Id')
            values = []
            for field in TABLES[table]:
                value = seconds if field == 'CreationDate' else month if field == 'month' else a.get(field)
                if value is None:
                    values.append('\\N')
                else:
                    number = int(value)
                    bits = 32 if field in ('PostTypeId', 'VoteTypeId', 'month') else 64
                    if not -(2 ** (bits - 1)) <= number < 2 ** (bits - 1):
                        raise ValueError('Integer overflow: ' + field)
                    values.append(str(number))
            if year not in counts:
                counts[year], parts[year] = 0, 0
                os.makedirs(os.path.join(dest, 'year=%d' % year))
            if counts[year] % chunk_rows == 0:
                if hasattr(os, 'statvfs'):
                    disk = os.statvfs(dest)
                    if disk.f_bavail * disk.f_frsize < reserve_bytes:
                        raise IOError('Low staging disk space; partial output retained')
                if year in handles:
                    handles[year].close()
                path = os.path.join(dest, 'year=%d' % year, 'part-%05d.tsv' % parts[year])
                handles[year] = open(path, 'wb')
                parts[year] += 1
            handles[year].write(('\t'.join(values) + '\n').encode('ascii'))
            counts[year] += 1
            first = seconds if first is None else min(first, seconds)
            elem.clear()
            root.clear()
    finally:
        for handle in handles.values():
            handle.close()
    return {'rows': sum(counts.values()), 'years': counts, 'first_epoch': first}


def archive_members(archive):
    output = subprocess.check_output(['7z', 'l', '-slt', archive]) if hasattr(subprocess, 'check_output') else None
    if output is None:  # Python 2.6
        process = subprocess.Popen(['7z', 'l', '-slt', archive], stdout=subprocess.PIPE)
        output = process.communicate()[0]
        if process.returncode:
            raise RuntimeError('7z listing failed')
    found = {}
    for line in output.decode('utf-8').splitlines():
        if line.startswith('Path = '):
            member = line[7:]
            for table in TABLES:
                if member.lower() == table + '.xml':
                    if table in found:
                        raise ValueError('Ambiguous XML member: ' + table)
                    found[table] = member
    if len(found) != 3:
        raise ValueError('Archive must contain posts/users/votes XML at root')
    return found


def prepare(root, archive_dir, site, dest):
    if not re.match(r'^[a-z0-9][a-z0-9.-]*$', site):
        raise ValueError('Invalid site')
    network = os.path.join(root, 'outputs', 'network')
    manifest_path = os.path.join(network, 'download_manifest.json')
    freeze_path = os.path.join(network, 'classification_freeze.json')
    freeze = read_json(freeze_path)
    if freeze['manifest_sha256'] != digest(manifest_path):
        raise ValueError('Manifest differs from frozen classification')
    if freeze['window_days'] != 7 or freeze['coverage_end_exclusive'] != '2024-03-01T00:00:00Z':
        raise ValueError('Unsupported metric contract')
    source = [x for x in read_json(manifest_path)['files'] if x['site'] == site]
    if len(source) != 1 or site not in freeze['sites']:
        raise ValueError('Site missing from frozen scope')
    source = source[0]
    if os.path.basename(source['name']) != source['name']:
        raise ValueError('Unsafe archive name')
    archive = os.path.join(archive_dir, source['name'])
    if os.path.getsize(archive) != source['bytes'] or digest(archive, 'sha1') != source['sha1']:
        raise ValueError('Source size/SHA1 mismatch; do not substitute another dump')
    entry = freeze['sites'][site]
    if entry['source_sha1'] != source['sha1']:
        raise ValueError('Frozen source mismatch')
    members = archive_members(archive)
    os.makedirs(dest)  # Deliberately fail for any existing/partial run.
    results = {}
    for table in ('posts', 'users', 'votes'):
        target = os.path.join(dest, table)
        os.makedirs(target)
        with open(os.path.join(dest, table + '-7z.log'), 'wb') as log:
            process = subprocess.Popen(['7z', 'x', '-so', archive, members[table]], stdout=subprocess.PIPE, stderr=log)
            try:
                results[table] = stream_table(process.stdout, table, target)
                process.stdout.close()
                if process.wait() != 0:
                    raise RuntimeError('7z extraction failed: ' + table)
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait()
    cutoff = epoch(freeze['coverage_end_exclusive'])[0]
    first = results['posts']['first_epoch']
    first_full = cutoff + 1
    if first is not None:
        dt = datetime.datetime.utcfromtimestamp(first)
        year, month = (dt.year + 1, 1) if dt.month == 12 else (dt.year, dt.month + 1)
        first_full = calendar.timegm(datetime.datetime(year, month, 1).timetuple())
    with open(os.path.join(dest, 'calendar.tsv'), 'wb') as stream:
        for offset in range(63):
            year, month = 2019 + offset // 12, offset % 12 + 1
            next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
            start = calendar.timegm(datetime.datetime(year, month, 1).timetuple())
            end = calendar.timegm(datetime.datetime(next_year, next_month, 1).timetuple())
            row = [site, entry['domain_group'], str(entry['is_meta']).lower(), entry['parent_site'] or '\\N',
                   str(year), str(month), '%04d-%02d' % (year, month),
                   str(start >= first_full and end <= cutoff).lower(),
                   str(start >= first_full and end + 604800 <= cutoff).lower()]
            stream.write(('\t'.join(row) + '\n').encode('ascii'))
    with open(os.path.join(dest, 'input.properties'), 'wb') as stream:
        props = {'site': site, 'source_sha1': source['sha1'], 'freeze_sha256': digest(freeze_path)}
        for table in results:
            props[table + '.years'] = ','.join(str(y) for y in sorted(results[table]['years']))
            props[table + '.rows'] = str(results[table]['rows'])
        for key in sorted(props):
            stream.write(('%s=%s\n' % (key, props[key])).encode('ascii'))
    write_json(os.path.join(dest, 'prepared.json'), {'complete': True, 'site': site,
               'source_sha1': source['sha1'], 'freeze_sha256': digest(freeze_path), 'tables': results})
