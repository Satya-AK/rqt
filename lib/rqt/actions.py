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


import os
import sys
import subprocess
import signal
import json
import argparse
import logging
import time
import datetime
import platform
import uuid
import getpass

from boto.s3.connection import S3Connection

from .version import __version__
from . import errors
from . import query_template
from .util import open_csv_writer


logger = logging.getLogger(__name__)


def get_config_filenm(args):
    """
    return the top-level config file name
    """
    default_config_filenm = os.path.expanduser("~/.rqt-config")
    config_filenm = default_config_filenm if args.config is None else args.config
    return config_filenm


def load_config(args):
    """
    load the top-level JSON configuration
    """
    config_filenm = get_config_filenm(args)
    if not os.path.exists(config_filenm):
        raise RQTMissingConfigError, config_filenm
    with open(config_filenm) as fp:
        config = json.load(fp)
    logger.info("loaded rqt config from %r" % config_filenm)
    return config


def get_conn_args(args):
    """
    return the connection args from the config
    """
    config = load_config(args)
    # Which connection key should be used?
    conn_key = "default" if args.connection is None else args.connection
    if conn_key not in config["connections"]:
        raise errors.RQTInvalidConnectionError, conn_key
    conn_args = config["connections"][conn_key]
    return conn_args


def get_connection(args):
    """
    get a Redshift connection
    """
    import psycopg2 # lazy import so show-query works without psycopg2
    conn_args = get_conn_args(args)
    conn = psycopg2.connect( database=conn_args["database"],
                             host=conn_args["server"], 
                             port=conn_args["port"], 
                             user=conn_args["user"], 
                             password=conn_args["password"],
                             # sslmode=???
                           )
    return conn


def setup_namespace(json_filenm):
    """
    return a parameter namespace from an optional json file and the environment
    """
    if json_filenm:
        tns = json.load(open(json_filenm))
        # Need to change Unicode keys into str keys.
        ns = {}
        for k, v in tns.items():
            ns[str(k)] = v
    else:
        ns = {}
    # Now add in the environment variables...
    for k, v in os.environ.items():
        # Don't override an JSON parameter with an environment variable.
        if k not in ns:
            ns[k] = v
    return ns


########################################################################
# Subcommand action functions
########################################################################


def do_create_config(args):
    """
    create an empty config
    """
    config_filenm = get_config_filenm(args)
    if os.path.exists(config_filenm):
        print >>sys.stdout, "RQT configuration file already exists at %r." % (config_filenm,)
        return
    data = { 
        "connections": {
            "default": {
                "server": "redshift server endpoint [REQUIRED]",
                "port": "redshift server port [REQUIRED]",
                "user": "redshift user [REQUIRED]",
                "password": "redshift password [REQUIRED]",
                "database": "redshift database [REQUIRED]",
                "query_group": "a_query_group_name {OPTIONAL]",
                "search_path": "path1, path2 [OPTIONAL]",
            }
        },
        "s3_usage_data": {
            "bucket": "S3 BUCKET FOR USAGE LOG",
            "key_prefix": "S3 BUCKET PREFIX FOR USAGE LOGS",
            "access_key_id": "AWS ACCESS KEY ID",
            "secret_access_key": "AWS SECRET ACCESS KEY"
        },
        "comments": [
            "...",
            "..."
        ]
    }
    with open(config_filenm, "w") as fp:
        json.dump(data, fp, indent=4)
        print >>fp
    print >>sys.stdout, "An empty RQT configuration file was created at %r." % (config_filenm,)


########################################################################


class SIGINTHandler:
    def __init__(self):
        self.calls = []
    def __call__(self, signal, frame):
        self.calls.append(time.time())
        if len(self.calls) > 3:
            if (self.calls[-1]-self.calls[-3]) < 1:
                raise SystemExit, 0
            else:
                self.calls = []


def do_run_psql(args):
    """
    run psql session to Redshift
    """
    config = load_config(args)
    conn_key = "default" if args.connection is None else args.connection
    if conn_key not in config["connections"]:
        raise errors.RQTInvalidConnectionError, conn_key
    conn_args = config["connections"][conn_key]
    os.environ["PGPASSWORD"] = conn_args["password"]
    cmd = [ 
        "psql",
        "-U", conn_args["user"],
        "-h", conn_args["server"],
        "-p", str(conn_args["port"]),
        "-d", conn_args["database"],
    ]
    if conn_args.get("search_path"):
        os.environ["PGOPTIONS"] = "-c search_path=%s" % (conn_args.get("search_path"),)
    logger.info("About to run %r." % (" ".join(cmd),))
    signal.signal(signal.SIGINT, SIGINTHandler())
    raise SystemExit, subprocess.call(cmd, env=os.environ)


########################################################################


def do_show_query(args):
    """
    show the expanded query template
    """
    ns = setup_namespace(args.json_params)
    q = query_template.expand_file(args.qt_filename, ns)
    print q


########################################################################


def do_show_plan(args):
    """
    show the query plan as per "explain" 
    """
    # Expand the query template.
    ns = setup_namespace(args.json_params)
    q = query_template.expand_file(args.qt_filename, ns)
    # Get the Redshift connection.
    conn = get_connection(args)
    cs = conn.cursor()
    # Set the query_group.
    conn_args = get_conn_args(args)
    query_group = _pick_query_group(args, conn_args)
    if query_group:
        cs.execute("SET query_group TO '%s';" % (query_group,))
        logger.info("SET query_group TO '%s';" % (query_group,))
    # Set the search_path.
    search_path = conn_args.get("search_path")
    if search_path is not None:
        cs.execute("SET search_path TO %s;" % (search_path,))
        logger.info("SET search_path TO %s;" % (search_path,))
    # Run the explain.
    cs.execute("explain "+q)
    # Write the plan to stdout.
    while 1:
        row = cs.fetchone()
        if row is None:
            break
        print row[0]


########################################################################


def _step1(cs, sql, run_log):
    """
    run the query and add some info to run_log
    """
    q_time_start = time.time()
    cs.execute(sql)
    q_time_end = time.time()
    q_time_elapsed = q_time_end - q_time_start
    logger.info("query_elapsed_seconds=%.1f row_count=%s" % (q_time_elapsed, cs.rowcount))
    run_log["row_count"] = cs.rowcount
    run_log["timing"]["query"] = {}
    run_log["timing"]["query"]["start"] = q_time_start
    run_log["timing"]["query"]["end"] = q_time_end
    run_log["timing"]["query"]["elapsed"] = q_time_elapsed


def _step2(cs, out_filenm, run_log):
    """
    write query results to file and some info to run_log
    """
    wo_time_start = time.time()
    # Open output...
    fp_out, wtr = open_csv_writer(out_filenm)
    # Write header row...
    col_nms = [desc[0] for desc in cs.description]
    wtr.writerow(col_nms)
    # Write query results to output...
    while 1:
        # FINISH: optimize with fetchmany()
        row = cs.fetchone()
        if row is None:
            break
        xrow = []
        for x in row:
            if type(x) is str:
                # I think the str's coming out of Redshift are
                # really UTF-8 byte arrays.  This may be related
                # to how the psycopg2 connection is set up.  Here 
                # they are converted into Python unicode string objects.
                xrow.append(x.decode("utf-8", "replace"))
            elif x is None:
                xrow.append(u"NULL")
            else:
                xrow.append(unicode(x))
        wtr.writerow(xrow)
    fp_out.close()
    wo_time_end = time.time()
    wo_time_elapsed = wo_time_end - wo_time_start
    logger.info("writeout_elapsed_seconds=%.1f" % (wo_time_elapsed,))
    run_log["timing"]["writeout"] = {}
    run_log["timing"]["writeout"]["start"] = wo_time_start
    run_log["timing"]["writeout"]["end"] = wo_time_end
    run_log["timing"]["writeout"]["elapsed"] = wo_time_elapsed
    if "stdout" not in out_filenm:
        run_log["result_size"] = os.stat(out_filenm).st_size
    else:
        run_log["result_size"] = 0
    logger.info("saved results to %r" % (out_filenm,))


def _run_select_to_file(cs, sql, out_filenm, run_log, args):
    # Run query...
    _step1(cs, sql, run_log)

    # Write-out the result...
    try:
        if cs.description and out_filenm != "/dev/null":
            # cs.description is None if the SQL did not return a result set.
            # out_filenm is /dev/null if the user doesn't want the result set written to a file.
            _step2(cs, out_filenm, run_log)
    finally:
        _write_run_log(run_log, args)


def _write_run_log(run_log, args):
    """
    write out the run_log to S3
    """
    config = load_config(args)
    if not config.get("s3_usage_data", {}):
        # Skip if configuration is empty...
        logger.info("no usage data logged")
        return
    # Establish the bucket key to write to...
    bucketname = config["s3_usage_data"]["bucket"]
    prefix = config["s3_usage_data"]["key_prefix"].lstrip("/").rstrip("/")
    cymd = datetime.date.today().strftime("%Y/%m/%d")
    keyname = "/".join((prefix, cymd, uuid.uuid4().hex))
    # Connect and write...
    s3 = S3Connection(config["s3_usage_data"]["access_key_id"],
                      config["s3_usage_data"]["secret_access_key"])
    bucket = s3.get_bucket(bucketname)
    key = bucket.new_key(keyname)
    key.set_contents_from_string(json.dumps(run_log, indent=4)+"\n")
    uri = "s3://%s/%s" % (bucketname, keyname)
    logger.info("usage data logged to %s" % (uri,))


def _pick_query_group(args, conn_args):
    if args.query_group:
        return args.query_group
    if conn_args.get("query_group"):
        return conn_args.get("query_group")
    return None 


def do_run_query(args):
    # Expand the query template.
    ns = setup_namespace(args.json_params)
    q = query_template.expand_file(args.qt_filename, ns)
    # Get the Redshift connection.
    conn = get_connection(args)
    cs = conn.cursor()
    # Set the query_group.
    conn_args = get_conn_args(args)
    query_group = _pick_query_group(args, conn_args)
    if query_group:
        cs.execute("SET query_group TO '%s';" % (query_group,))
        logger.info("SET query_group TO '%s';" % (query_group,))
    # Set the search_path.
    search_path = conn_args.get("search_path")
    if search_path is not None:
        cs.execute("SET search_path TO %s;" % (search_path,))
        logger.info("SET search_path TO %s;" % (search_path,))
    # Start a "run log" dictionary.
    run_log = {}
    run_log["version"] = "1"
    run_log["os_user"] = getpass.getuser()
    run_log["hostname"] = platform.node()
    run_log["conn_args"] = conn_args.copy()
    del run_log["conn_args"]["password"] # don't log the password!
    run_log["query_template"] = open(args.qt_filename).read()
    run_log["query"] = q
    run_log["query_group"] = query_group
    run_log["search_path"] = search_path
    run_log["timing"] = {}
    # Execute the query.
    # FINISH: verify the output file extension makes sense.
    _run_select_to_file(cs, q, args.out_filename, run_log, args)



