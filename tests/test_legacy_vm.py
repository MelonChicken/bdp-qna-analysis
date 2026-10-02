"""Host checks for legacy staging and comparison; no VM claim."""
import io
from pathlib import Path
import sys
import tempfile
import unittest

LEGACY = Path(__file__).resolve().parents[1] / 'vm/legacy'
sys.path.insert(0, str(LEGACY))
from prepare import stream_table
from run_site import compare


class LegacyTests(unittest.TestCase):
    def test_utc_null_year_and_chunks(self):
        xml = b'''<posts><row Id="1" PostTypeId="1" CreationDate="2022-12-31T23:59:59.999"/>
        <row Id="2" PostTypeId="2" ParentId="1" CreationDate="2023-01-01T00:00:00"/>
        <row Id="3" PostTypeId="2" ParentId="1" CreationDate="2023-01-08T00:00:00"/></posts>'''
        with tempfile.TemporaryDirectory() as tmp:
            result = stream_table(io.BytesIO(xml), 'posts', tmp, chunk_rows=1, reserve_bytes=0)
            self.assertEqual(result['rows'], 3)
            self.assertEqual(len(list(Path(tmp).glob('year=*/*.tsv'))), 3)
            first = (Path(tmp)/'year=2022/part-00000.tsv').read_text().strip().split('\t')
            self.assertEqual(first, ['1','1','1672531199','\\N','12'])
            self.assertEqual(result['years'], {2022: 1, 2023: 2})

    def test_empty_and_bad_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(stream_table(io.BytesIO(b'<votes/>'), 'votes', tmp)['rows'], 0)
        for xml in [b'<users><row CreationDate="2020-01-01T00:00:00"/></users>',
                    b'<posts/>', b'<users><row Id="1" CreationDate="bad"/></users>',
                    b'<users><row Id="9223372036854775808" CreationDate="2020-01-01T00:00:00"/></users>']:
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises((ValueError, KeyError)):
                    stream_table(io.BytesIO(xml), 'users', tmp)

    def test_compare_omitted_nulls_duplicates_nan(self):
        reference = [{'site':'example.com','month':str(i),'answered_rate':None,'questions_count':0} for i in range(63)]
        actual = [{'site':'example.com','month':str(i),'questions_count':0} for i in range(63)]
        self.assertTrue(compare(actual, reference, 'example.com')['complete'])
        with self.assertRaises(ValueError):
            compare(actual[:-1] + [actual[0]], reference, 'example.com')
        reference[0]['answered_rate'] = 0.5
        actual[0]['answered_rate'] = float('nan')
        with self.assertRaises(ValueError):
            compare(actual, reference, 'example.com')
        actual[0]['answered_rate'] = 0.5
        actual[1]['questions_count'] = None
        with self.assertRaises(ValueError):
            compare(actual, reference, 'example.com')


if __name__ == '__main__':
    unittest.main()
