import unittest
import sys
import datetime

from dateutil.tz import tzutc

import gpstime

##################################################

def cu_date_available():
    """return True if coreutils date parser available"""
    try:
        gpstime._cu_date_parse()
        return True
    except:
        return False


class TestGPStime(unittest.TestCase):

    def roundtrip_gps(self, gps):
        self.assertEqual(gpstime.unix2gps(gpstime.gps2unix(gps)), gps)

    def roundtrip_unix(self, unix):
        self.assertEqual(gpstime.gps2unix(gpstime.unix2gps(unix)), unix)

    ##########

    def test_conversion(self):
        self.assertEqual(gpstime.gps2unix(1133585676), 1449550459)
        self.assertEqual(gpstime.unix2gps(1449550459), 1133585676)
        self.assertEqual(1133585676, gpstime.unix2gps(gpstime.gps2unix(1133585676)))

    def test_conversion_past(self):
        self.assertEqual(gpstime.gps2unix(123456789), 439421586)
        self.assertEqual(gpstime.unix2gps(439421586), 123456789)

    def test_conversion_onleap(self):
        self.assertEqual(gpstime.gps2unix(425520006), 741484798)
        self.assertEqual(gpstime.gps2unix(425520007), 741484799)
        self.assertEqual(gpstime.gps2unix(425520008), 741484799)
        self.assertEqual(gpstime.gps2unix(425520009), 741484800)
        self.assertEqual(gpstime.unix2gps(741484798), 425520006)
        self.assertEqual(gpstime.unix2gps(741484799), 425520007)
        self.assertEqual(gpstime.unix2gps(741484800), 425520009)

    def test_roundtrip(self):
        self.roundtrip_gps(123456789)
        self.roundtrip_unix(439421586)

    @unittest.expectedFailure
    def test_roundtrip_onleap(self):
        self.roundtrip_gps(425520008)

    def test_gpstime_new(self):
        self.assertEqual(gpstime.gpstime(2015, 12, 8, 4, 54, 19, 0, tzutc()).gps(),
                         1133585676.0)

    def test_gpstime_fromdatetime(self):
        dt = datetime.datetime(2015, 12, 8, 4, 54, 19, 0, tzutc())
        gt = gpstime.gpstime.fromdatetime(dt)
        self.assertIsInstance(gt, gpstime.gpstime)
        self.assertEqual(gt.gps(), 1133585676.0)

    def test_gpstime_parse_now_roundtrip(self):
        gt0 = gpstime.gpstime.parse()
        gt1 = gpstime.gpstime.parse(gt0.gps())
        self.assertEqual(gt0, gt1)

    def test_gpstime_parse_utc(self):
        self.assertEqual(gpstime.gpstime.parse('Dec 08 2015 04:54:19.2 UTC').gps(),
                         1133585676.2)

    def test_gpstime_parse_utc2(self):
        self.assertEqual(gpstime.gpstime.parse('2014-07-03 17:16:14 UTC').gps(),
                         1088442990.0)

    def test_gpstime_parse_local(self):
        self.assertEqual(gpstime.gpstime.parse('Dec 08 2015 04:54:19.2 PST').gps(),
                         1133614476.2)

    def test_gpstime_parse_gps(self):
        self.assertEqual(gpstime.gpstime.parse(1133585676.2).gps(),
                         1133585676.2)

    def test_gpstime_parse_gps_iso(self):
        self.assertEqual(gpstime.gpstime.parse(1133585676.2).iso(),
                         '2015-12-08T04:54:19.200000Z')

    def test_gpstime_parse_iso(self):
        self.assertEqual(gpstime.gpstime.parse('2015-12-08T04:54:19.200000Z').iso(),
                         '2015-12-08T04:54:19.200000Z')

    def test_gpstime_parse_iso_alt(self):
        self.assertEqual(gpstime.gpstime.parse('2015-12-08_04:54:19.200000Z').iso(),
                         '2015-12-08T04:54:19.200000Z')

    @unittest.skipIf(not cu_date_available(), "coreutils date not available")
    def test_gpstime_parse_timestamp(self):
        self.assertEqual(gpstime.gpstime.parse('@1474821047').gps(),
                         1158856264)

    @unittest.skipIf(not cu_date_available(), "coreutils date not available")
    def test_gpstime_parse_now0(self):
        self.assertIsInstance(gpstime.gpstime.parse(), gpstime.gpstime)

    @unittest.skipIf(not cu_date_available(), "coreutils date not available")
    def test_gpstime_parse_now1(self):
        self.assertIsInstance(gpstime.gpstime.parse('now'), gpstime.gpstime)

    @unittest.skipIf(not cu_date_available(), "coreutils date not available")
    def test_gpstime_parse_relative0(self):
        self.assertIsInstance(gpstime.gpstime.parse('yesterday'), gpstime.gpstime)

    @unittest.skipIf(not cu_date_available(), "coreutils date not available")
    def test_gpstime_parse_relative1(self):
        self.assertIsInstance(gpstime.gpstime.parse('1 month ago'), gpstime.gpstime)

    def test_gpstime_parse_tconvert(self):
        self.assertEqual(gpstime.gpstime.tconvert('2015-12-08T04:54:19.200000Z'),
                         gpstime.gpstime.tconvert(1133585676.2))

    def test_gpstime_fromgps(self):
        self.assertEqual(gpstime.gpstime.fromgps(1133585676).iso(),
                         '2015-12-08T04:54:19.000000Z')

    def test_gpstime_fromgps_rounding(self):
        self.assertEqual(gpstime.gpstime.fromgps(1133585676.274).iso(),
                         '2015-12-08T04:54:19.274000Z')
        self.assertEqual(gpstime.gpstime.fromgps(1133585676.874).iso(),
                         '2015-12-08T04:54:19.874000Z')

    def test_gpstime_fromgps_timestamp(self):
        self.assertEqual(gpstime.gpstime.fromgps(1133585676).timestamp(),
                         1449550459)

    def test_gpstime_tconvert_classmethod(self):
        self.assertEqual(gpstime.gpstime.tconvert(1133585676.2).gps(),
                         1133585676.2)

    def test_gpstime_tconvert_iso(self):
        self.assertEqual(gpstime.tconvert('2015-12-08T04:54:19.200000Z'),
                         1133585676.2)

    def test_gpstime_tconvert_gps(self):
        self.assertEqual(gpstime.tconvert(1133585676.2),
                         '2015-12-08 04:54:19.200000 UTC')

    @unittest.expectedFailure
    def test_gpstime_parse_leap(self):
        self.assertEqual(gpstime.gpstime.parse('Jun 30 1993 23:59:60 UTC').gps(),
                         425520008)

    @unittest.expectedFailure
    def test_gpstime_parse_gps_leap(self):
        self.assertEqual(gpstime.gpstime.parse(425520008).iso(),
                         '1993-06-30T23:59:60.000000Z')

    @unittest.skipIf(sys.version_info < (3, 8), "returns datetime in python < 3.8")
    def test_gpstime_add(self):
        dt = gpstime.gpstime.parse(1133585676.2)
        delta = datetime.timedelta(seconds=1)
        self.assertTrue(isinstance(dt + delta, gpstime.gpstime))
        dt += delta
        self.assertTrue(isinstance(dt, gpstime.gpstime))

    @unittest.skipIf(sys.version_info < (3, 8), "returns datetime in python < 3.8")
    def test_gpstime_subtract(self):
        dt = gpstime.gpstime.parse(1133585676.2)
        delta = datetime.timedelta(seconds=1)
        self.assertTrue(isinstance(dt - delta, gpstime.gpstime))
        dt -= delta
        self.assertTrue(isinstance(dt, gpstime.gpstime))

##################################################

if __name__ == '__main__':
    unittest.main(verbosity=5, failfast=False, buffer=True)
