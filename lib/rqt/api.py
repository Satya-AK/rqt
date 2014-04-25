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


import rqt.cli_parser
import rqt.actions


def get_conn(conn_name):
    """
    return a pycopg2 DB connection by name using the .rqt-config
    """
    argv = ["--connection", conn_name, "run-psql"] # run-psql is a placeholder so parser will work
    parser = rqt.cli_parser.mk_argparser()
    args = parser.parse_args(argv)
    conn = rqt.actions.get_connection(args)
    return conn


