#  Copyright 2014 Accuen
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import sys
import gzip
import cStringIO
import csv
import codecs



class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.

    The row passed to .writerow() should be Python 
    strings containing ASCII or Python unicode strings.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def open_csv_writer(filenm):
    """
    returns a fileobj and csv.writer
    """
    # stdout is a special file...
    if filenm.startswith("stdout"):
        fp1 = sys.stdout
    else:
        fp1 = open(filenm, "w")
    # May need to wrap in a GzipFile...
    if filenm.endswith(".gz"):
        fp2 = gzip.GzipFile(fileobj=fp1, mode="w", compresslevel=6)
        filenm2 = filenm[:-3]
    else:
        fp2 = fp1
        filenm2 = filenm
    # Pick CSV or tab-delim output...
    if filenm2.endswith(".csv"):
        wtr = UnicodeWriter(fp2, dialect="excel")
    elif filenm2.endswith(".txt"):
        wtr = UnicodeWriter(fp2, dialect="excel-tab")
    else:
        raise ValueError, "unsupported file type: %r" % filenm
    # Return the file object for closing and the writer for writing...
    return fp2, wtr


