# -*- coding: utf-8 -*-
"""Run inside the course VM: Python 2.6+, p7zip, Hadoop and Spark 1.3."""
from __future__ import with_statement
import datetime
import json
import math
import os
from optparse import OptionParser
import re
import subprocess
import sys
from prepare import prepare, read_json, write_json, digest


def capture(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE)
    out = p.communicate()[0]
    if p.returncode:
        raise RuntimeError('Command failed (%s): %s' % (p.returncode, args))
    return out.decode('utf-8')


def compare(actual, reference, site):
    expected = [r for r in reference if r['site'] == site]
    if len(expected) != 63 or len(actual) != 63:
        raise ValueError('Expected 63 reference and actual rows')
    lookup = dict(((r['site'], r['month']), r) for r in actual)
    if len(lookup) != 63:
        raise ValueError('Duplicate monthly keys')
    for old in expected:
        key = (site, old['month'])
        if key not in lookup:
            raise ValueError('Missing month: ' + str(key))
        new = lookup[key]
        for field, value in old.items():
            got = new.get(field)  # Spark JSON omits NULL fields.
            if field in ('answered_rate', 'downvote_ratio') and value is not None and got is not None:
                if math.isnan(got) or math.isinf(got) or abs(value - got) > 1e-12:
                    raise ValueError('Ratio mismatch: %s %s' % (key, field))
            elif value != got:
                raise ValueError('Mismatch: %s %s: %r != %r' % (key, field, value, got))
    return {'complete': True, 'site': site, 'rows': 63, 'absolute_ratio_tolerance': 1e-12}


def main():
    parser = OptionParser(usage='%prog --project ROOT --archives DIR --site HOST --work DIR --hdfs-run /user/USER/unique-run')
    for name in ('project', 'archives', 'site', 'work', 'hdfs-run'):
        parser.add_option('--' + name, dest=name.replace('-', '_'))
    parser.add_option('--driver-memory', default='768m')
    options, args = parser.parse_args()
    if args or not all(getattr(options, n) for n in ('project','archives','site','work','hdfs_run')):
        parser.error('All five path/site options are required')
    if not re.match(r'^/[a-zA-Z0-9_-]+(?:/[a-zA-Z0-9_.=-]+)+$', options.hdfs_run) or '..' in options.hdfs_run.split('/'):
        parser.error('Use an absolute HDFS path with safe components and a unique run name')
    if not re.match(r'^[a-z0-9][a-z0-9.-]*$', options.site):
        parser.error('Invalid site')
    if not re.match(r'^[1-9][0-9]*[mg]$', options.driver_memory):
        parser.error('Driver memory must be e.g. 768m')
    for command in ('7z', 'hdfs', 'spark-shell', 'java'):
        if subprocess.call(['which', command], stdout=subprocess.PIPE) != 0:
            raise RuntimeError('Required command missing: ' + command)
    root = os.path.abspath(options.project)
    work = os.path.abspath(options.work)
    if not os.path.isdir(work):
        raise ValueError('Create a writable work directory before starting')
    disk = os.statvfs(work)
    if disk.f_bavail * disk.f_frsize < 2 * 1024 ** 3:
        raise ValueError('Need at least 2 GiB staging free for the initial small-site trial; this is not a full-run sizing guarantee')
    hroot = options.hdfs_run + '/' + options.site
    # Test connection separately so a connection failure cannot look like a missing path.
    capture(['hdfs', 'dfs', '-ls', '/'])
    capture(['hdfs', 'dfs', '-df', '-h', '/'])
    exists = subprocess.call(['hdfs','dfs','-test','-e',hroot])
    if exists != 1:
        raise ValueError('HDFS destination exists or existence check failed: ' + hroot)
    local = os.path.join(work, os.path.basename(options.hdfs_run) + '-' + options.site)
    if os.path.exists(local):
        raise ValueError('Work output exists; use a new run name: ' + local)
    # Hash the implementation and reference before starting, for a durable run receipt.
    implementation = os.path.dirname(os.path.abspath(__file__))
    hashes = dict((n, digest(os.path.join(implementation,n))) for n in ('prepare.py','run_site.py','run.scala','metrics.sql'))
    reference_path = os.path.join(root,'outputs','network','monthly_metrics_audit.json')
    reference_hash = digest(reference_path)
    prepare(root, os.path.abspath(options.archives), options.site, local)
    subprocess.check_call(['hdfs','dfs','-mkdir','-p',hroot])
    subprocess.check_call(['hdfs','dfs','-put',local,hroot + '/input'])
    env = os.environ.copy()
    env.update({'VM_INPUT':hroot + '/input','VM_OUTPUT':hroot + '/output',
                'VM_PROPERTIES':os.path.join(local,'input.properties'),
                'VM_SQL':os.path.join(implementation,'metrics.sql'), 'TZ':'UTC'})
    scratch = os.path.join(local,'spark-tmp')
    os.mkdir(scratch)
    command = ['spark-shell','--master','local[1]','--driver-memory',options.driver_memory,
               '--conf','spark.local.dir=' + scratch,
               '--conf','spark.ui.enabled=false',
               '--conf','spark.driver.extraJavaOptions=-Duser.timezone=UTC',
               '-i',os.path.join(implementation,'run.scala')]
    with open(os.path.join(local,'spark.log'),'wb') as log:
        process = subprocess.Popen(command, cwd=local, env=env, stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT)
        process.communicate(b':quit\n')
    if process.returncode:
        raise RuntimeError('Spark failed; inspect ' + os.path.join(local,'spark.log'))
    # spark-shell can return zero after compilation errors; require our own marker.
    marker = capture(['hdfs','dfs','-cat',hroot + '/output/_VM_SUCCESS']).splitlines()
    prepared = read_json(os.path.join(local,'prepared.json'))
    if marker != [options.site, prepared['freeze_sha256']]:
        raise ValueError('Wrong or missing Spark success receipt')
    output = capture(['hdfs','dfs','-cat',hroot + '/output/audit/part-*'])
    actual = [json.loads(line) for line in output.splitlines() if line.strip()]
    if digest(reference_path) != reference_hash:
        raise ValueError('Reference changed during execution')
    result = compare(actual, read_json(reference_path), options.site)
    result.update({'hdfs_output':hroot + '/output', 'implementation_sha256':hashes,
                   'reference_sha256':reference_hash, 'source_sha1':prepared['source_sha1'],
                   'freeze_sha256':prepared['freeze_sha256'],
                   'finished_utc':datetime.datetime.utcnow().isoformat() + 'Z'})
    write_json(os.path.join(local,'audit.json'),actual)
    write_json(os.path.join(local,'validated.json'),result)
    print(json.dumps(result,sort_keys=True))


if __name__ == '__main__':
    main()
