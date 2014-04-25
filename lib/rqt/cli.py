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


"""
CLI for Redshift queries with templates
"""
import sys
import logging
import boto

from . import cli_parser 
from .version import __version__


logger = logging.getLogger(__name__)


def setup_logging():
    log_format = "[%(process)d]%(name)s:%(levelname)s:%(asctime)s %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format=log_format, datefmt=date_format, level=logging.INFO)
    #logging.getLogger("bqt.google_api").setLevel(logging.WARNING) # for future reference


def main():
    if boto.config.has_section("Boto"):
        # Having this set to True caused some problems with S3 at some point.
        # Maybe it still does.
        boto.config.set("Boto", "https_validate_certificates", "False")

    # Maybe this is nicer...
    #if len(sys.argv) == 1:
    #    raise SystemExit, "rqt: missing command; try 'rqt -h' or 'rqt help'"

    setup_logging()
    logger.info("version==%r" % (__version__,))

    parser = cli_parser.mk_argparser()

    # Change the log level of some loggers for --debug.
    #logging.getLogger("rqt").setLevel(logging.DEBUG if args.debug else logging.INFO) # for future reference
    #logging.getLogger("rqt.save_result").setLevel(logging.DEBUG if args.debug else logging.INFO) # for future reference

    args = parser.parse_args(sys.argv[1:])
    args.func(args)

    return


