import os
import sys
import time
import calendar
import warnings
from tempfile import NamedTemporaryFile
from functools import lru_cache

import appdirs


CACHE_DIR = appdirs.user_cache_dir('gpstime')

LEAP_FILES = [
    '/usr/share/zoneinfo/leap-seconds.list',
    '/usr/share/zoneinfo/leapseconds',
    os.path.join(CACHE_DIR, 'leap-seconds.list'),
    os.path.join(CACHE_DIR, 'leapseconds')
]

SOURCES = [
    'https://hpiers.obspm.fr/iers/bul/bulc/ntp/leap-seconds.list',
    'https://data.iana.org/time-zones/tzdb/leapseconds',
    # NOTE: these were not working the last i checked
    # 'ftp://boulder.ftp.nist.gov/pub/time/leap-seconds.list',
    # 'ftp://ftp.nist.gov/pub/time/leap-seconds.list',
]


def ntp2unix(ts):
    """Convert NTP timestamp to UTC UNIX timestamp

    1900-01-01T00:00:00Z -> 1970-01-01T00:00:00Z

    """
    return int(ts) - 2208988800


def load_IANA(path):
    """Parse the `leapseconds` file format used by IANA.

    """
    data = []
    expires = 0
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line[:8] == '#expires':
                expires = int(line.split()[1])
            elif line[0] == '#':
                continue
            else:
                year, mon, day, ts, correction = line.split()[1:6]
                st = time.strptime(
                    '{} {} {} {}'.format(year, mon, day, ts),
                    '%Y %b %d %H:%M:%S',
                )
                # FIXME: do something with correction
                data.append(calendar.timegm(st))
    return data, expires


def load_IERS(path):
    """Parse the leap-seconds.list file format used by NIST, IERS, and IETF.

    """
    data = []
    expires = 0
    first = True
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            elif line[:2] == '#@':
                expires = ntp2unix(line.split()[1])
            elif line[0] == '#':
                continue
            else:
                # ignore the first entry since that doesn't
                # actually correspond to a leap second
                if first:
                    first = False
                    continue
                leap, offset = line.split()[:2]
                # FIXME: do something with offset
                data.append(ntp2unix(leap))
    return data, expires


def _download_file(url, fobj):
    """Download file over HTTP(S) or FTP, write to file object

    """
    from urllib.parse import urlparse

    parts = urlparse(url)

    if parts.scheme in ['http', 'https']:
        import requests

        r = requests.get(url, timeout=10)
        r.raise_for_status()
        for c in r.iter_content():
            fobj.write(c)

    elif parts.scheme in ['ftp']:
        import ftplib

        with ftplib.FTP(host=parts.hostname, timeout=10) as ftp:
            ftp.login()
            ftp.retrbinary(f"RETR {parts.path}", fobj.write)

    else:
        raise RuntimeError(f"Unsupported schema: {parts.scheme}")


class LeapData:
    """Leap second data class

    """
    _GPS0 = 315964800

    def __init__(self, path):
        """Load leap second data from file

        """
        base, ext = os.path.splitext(path)

        if ext == '':
            load_func = load_IANA
        elif ext == '.list':
            load_func = load_IERS
        else:
            raise RuntimeError("Unknown leap file extension/type.")

        try:
            data, expires = load_func(path)
        except Exception as e:
            raise RuntimeError(f"Error parsing leap file {path}: {str(e)}")

        if not data:
            raise ValueError("Leap second data is empty.")

        self._data = data
        self.expires = expires

    @property
    def data(self):
        """Return leap second data with times represented as UNIX.

        """
        if self.expired:
            warnings.warn("Leap second data is expired.", RuntimeWarning)
        return self._data

    @property
    def expired(self):
        """True if leap second data is expired

        """
        return self.expires <= time.time()

    @property
    def valid(self):
        """True if leap second data is available and not expired

        """
        return self._data and not self.expired

    def __iter__(self):
        for leap in self.data:
            yield leap

    @lru_cache(maxsize=None)
    def as_gps(self):
        """Returns leap second data with times represented as GPS.

        """
        leaps = [(leap - self._GPS0) for leap in self.data if leap >= self._GPS0]
        return [(leap + i) for i, leap in enumerate(leaps)]

    @lru_cache(maxsize=None)
    def as_unix(self, since_gps_epoch=False):
        """Return leap second data with times represented as UNIX.

        If since_gps_epoch is set to True, only return leap second
        data since the GPS epoch (1980-01-06T00:00:00Z).

        """
        if since_gps_epoch:
            return [leap for leap in self.data if leap >= self._GPS0]
        else:
            return list(self.data)


def find_leap_data(download=None):
    """Find leap second data

    System and user cache locations will be searched.  If a local file
    is not found, one will be downloaded from the canonical online
    source.

    There are three options for the `download` argument:

    True: System leap data will be ignored and the leap data file will
      be downloaded from the remote source.

    False: Only system leap data will used.

    None (default): The first available leap data will be used, and it
      will be downloaded if the system leap data is not available or
      invalid.

    Downloaded leap data will be stored in the local user cache
    directory.

    """
    leapdata = None

    if download is not True:
        for path in LEAP_FILES:

            if not os.path.exists(path):
                continue

            try:
                leapdata = LeapData(path)
            except Exception:
                continue

            if leapdata.valid:
                return leapdata

    if download is not False:
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)

        for remote in SOURCES:
            print(f"Attempting to update user leap data cache from {remote}...", file=sys.stderr)

            filename = os.path.basename(remote)
            path = os.path.join(CACHE_DIR, filename)
            fobj = NamedTemporaryFile(suffix=f'_{filename}', dir=CACHE_DIR, delete=False)
            temp_path = fobj.name

            try:
                _download_file(remote, fobj)
            except Exception:
                fobj.close()
                os.remove(temp_path)
                continue
            else:
                fobj.close()

            try:
                leapdata = LeapData(temp_path)
            except Exception:
                os.remove(temp_path)
                continue

            if os.path.exists(path):
                os.remove(path)
            os.rename(temp_path, path)

            if leapdata.valid:
                return leapdata

        else:
            print("Leap second data could not be downloaded.", file=sys.stderr)

    if leapdata:
        return leapdata

    raise RuntimeError("Leap file could not be found.")


LEAPDATA = find_leap_data()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="print leap second info")
    parser.add_argument(
        '--force', '-f', action='store_true',
        help="force download leap data")
    args = parser.parse_args()
    if args.force:
        LEAPDATA = find_leap_data(download=True)
    print("expires: {}".format(LEAPDATA.expires))
    for ls in LEAPDATA:
        print(ls)
