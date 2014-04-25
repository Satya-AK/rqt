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
import argparse
import logging
from textwrap import dedent


from .version import __version__
from . import actions
from .help import mk_help_text


logger = logging.getLogger(__name__)


COMMANDS = [
    "help",
    "create-config",
    "run-query",
    "show-query",
    "show-plan",
    "run-psql",
]


def do_help(*pargs, **kwargs):
    print mk_help_text(COMMANDS)


def add_help_subparser(subparsers):
    parser = subparsers.add_parser("help",
                                   help="Display extended help.",
                                   add_help=False # no -h for help command
                                   )
    parser.set_defaults(func=do_help)
    # FINISH: Make "pgm help command" work as an alias for "pgm command -h".
    return parser


def add_create_config_subparser(subparsers):
    description = dedent("""\
        Creates an uninitialized configuration file if none exists.
        The file is written to ~/.rqt-config.
    """)
    parser = subparsers.add_parser("create-config",
                                   description=description,
                                   help="Creates a configuration file if none exists.")

    parser.set_defaults(func=actions.do_create_config)
    return parser


def add_show_query_subparser(subparsers):
    description = dedent("""\
        Shows the query resulting from template expansion.
    """)
    parser = subparsers.add_parser("show-query",
                                   description=description,
                                   help="Shows the query resulting from template expansion.")
    parser.set_defaults(func=actions.do_show_query)
 
    parser.add_argument("qt_filename", 
                        metavar="QUERY_FILE", 
                        help="the query template file")

    # To do: New feature; command line defines
    #parser.add_argument("--define", "-d", 
    #                    action="append", 
    #                    metavar="K=V", 
    #                    help="add var. defn. to namespace")

    parser.add_argument("--json_params", "-p", 
                        metavar="JSON_FILE",
                        help="JSON file containing variables to add to the template namespace")

    return parser


def add_show_plan_subparser(subparsers):
    description = dedent("""\
        Shows the engine's query execution plan.
    """)
 
    parser = subparsers.add_parser("show-plan",
                                   description=description,
                                   help="Shows the engine's query execution plan.")
    parser.set_defaults(func=actions.do_show_plan)
 
    parser.add_argument("qt_filename", metavar="QUERY_FILE", help="the query template file")

    parser.add_argument("--json_params", "-p", metavar="JSON_FILE",
        help="JSON file containing variables to add to the template namespace")

    return parser


def add_run_query_subparser(subparsers):
    description = dedent("""\
        Runs a query and downloads result to a file.")
    """)
 
    parser = subparsers.add_parser("run-query",
                                   description=description,
                                   help="Runs a query and downloads result to a file.")
    parser.set_defaults(func=actions.do_run_query)
 
    parser.add_argument("qt_filename", metavar="QUERY_FILE", help="the query template file")
    parser.add_argument("out_filename", metavar="OUT_FILE", help="the output file (must have .csv extension)")

    parser.add_argument("--json_params", metavar="JSON_FILE",
        help="JSON file containing variables to add to the template namespace")

    return parser


def add_run_psql_subparser(subparsers):
    description = dedent("""\
        Starts up a psql session with Redshift.
    """)
 
    parser = subparsers.add_parser("run-psql",
                                   description=description,
                                   help="Starts up a psql session with Redshift.")
    parser.set_defaults(func=actions.do_run_psql)
 
    return parser


def mk_argparser():
    desc = "Utility for running Redshift queries."

    epi = dedent("""\
        Try "rqt SUBCOMMAND -h" for help on a specific subcommand.
        Try "rqt help" for extended help.
    """)
    parser = argparse.ArgumentParser(description=desc, 
                                     epilog=epi, 
                                     formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--config", metavar="JSON_FILE", default=None,
        help="JSON file containing main configuration")

    parser.add_argument("--debug", action="store_true", default=False,
        help="display debug level log messages")

    parser.add_argument("--verbose", action="store_true", default=False,
        help="display debug level log messages")

    parser.add_argument("--query_group", 
                        metavar="GROUP",
                        help="the Redshift query_group to use (default is taken from config)")
                        
    parser.add_argument("--connection", metavar="CONNECTION", 
                        help="the connection parameters to use from the config (default is taken from config)", 
                        default="default")

    metavar = "SUBCOMMAND"
    subparsers = parser.add_subparsers(description="Use 'rqt SUBCOMMAND ...' to run rqt.",
                                       dest="mode", metavar=metavar)

    add_help_subparser(subparsers)
    add_create_config_subparser(subparsers)
    add_run_query_subparser(subparsers)
    add_show_query_subparser(subparsers)
    add_show_plan_subparser(subparsers)
    add_run_psql_subparser(subparsers)

    return parser


