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


from textwrap import dedent


def mk_help_text(commands):
    help_text = dedent("""\
        = rqt - Utility for running Redshift queries =

        Use 'rqt -h' for a brief summary of options and subcommands.

        Use 'rqt SUBCOMMAND -h' for a brief summary of SUBCOMMAND and its options.

        The subcommands are: %s

        The extended help for bqt hasn't been written, but this might help.

        == rqt Features ==
            * download query result to CSV file
            * template with Jinja2 or Mako; template engine auto-detection
            * expand template without execution (using show-query)
            * view query plan (using show-plan)
            * manage connection params via config file
            * use default WLM query_group via config file or option (--query_group=GROUP)

        == rqt quick reference ==
            * Global options:
                * --config=CONFIG_FILE
                    * specify the config file
                    * defaults to ~/.rqt-config
                * --debug
                    * show debug output
                * --query_group=GROUP
                    * define WLM query_group to use (default is from config)
            * Create a config file:
                * rqt create-config
                    * Creates ~/.rqt-config if it does not exist.
            * Run a query:
                * rqt run-query QUERY_FILE OUTPUT_FILE [--json_params=PARAMS_FILE] [--query_group=GROUP]
            * Show a query after template expansion:
                * rqt show-query QUERY_FILE [--json_params=PARAMS_FILE]
            * Show a query plan:
                * rqt show-plan QUERY_FILE [--json_params=PARAMS_FILE]
            * Start a psql session:
                * rqt run-psql
    """ % (", ".join(commands)) )
    return help_text


