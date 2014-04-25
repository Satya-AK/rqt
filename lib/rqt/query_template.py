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
expand functions for query templates

"""
import os
import sys

try:
    from psycopg2.extensions import adapt
except:
    adapt = None

import jinja2
import mako.template
import mako.exceptions
import pystache


class RQTTemplateExpansionError(Exception):
    pass


def _detect_template_engine(s):
    # Use she-bang lines to be explicit.
    if s.startswith("#!jinja2"):
        return "jinja2"
    if s.startswith("#!mako"):
        return "mako"
    if s.startswith("#!mustache"):
        return "pystache"
    return "jinja2"
    # Auto-detection hints... not implemented
    # common_mako = ("<%", "%>", "${")
    # common_jinja2 = ("{{", "}}", "{%", "%}")


def expand_file(filenm, namespace):
    """
    returns the template-completed query from a file
    """
    if not os.path.exists(filenm):
        # FINISH: use custom exception class
        raise ValueError, "file not found; %r" % filenm

    with open(filenm) as fp:
        s = fp.read()

    return expand_str(s, namespace)


def _trim_shebang(s):
    if not s.startswith("#!"):
        # No she-bang present...
        return s
    parts = s.split("\n", 1)
    # Special case for only one line 
    if len(parts) == 1:
        parts.append("")
    parts[0] = "" # Replace she-bang line with empty line.
    return "\n".join(parts)


def expand_str(s, namespace):
    """
    returns the template-completed query from a string
    """
    kind = _detect_template_engine(s)
    s = _trim_shebang(s)
    if kind == "jinja2":
        return jinja2_expand_str(s, namespace)
    elif kind == "mako":
        return mako_expand_str(s, namespace)
    elif kind == "mustache":
        return pystache_expand_str(s, namespace)
    else:
        # Default to using Jinja2.
        return jinja2_expand_str(s, namespace)


########################################################################
########################################################################


def mako_expand_str(s, namespace):
    if adapt is not None and "adapt" not in namespace:
        # Add the "SQL quoting" Pyscopg2 adapt function.
        namespace["adapt"] = lambda v: adapt(v).getquoted()
    try:
        templ = mako.template.Template(s)
        return templ.render(**namespace)
    except:
        raise
        traceback = mako.exceptions.RichTraceback()
        raise traceback
        for (filename, lineno, function, line) in traceback.traceback:
            print >>sys.stderr, "File %s, line %s, in %s" % (filename, lineno, function)
            print >>sys.stderr, line, "\n"
        print >>sys.stderr, "%s: %s" % (str(traceback.error.__class__.__name__), traceback.error)
        raise RQTTemplateExpansionError, "template expansion failed"


########################################################################
########################################################################


def pystache_expand_str(s, namespace):
    return pystache.render(s, namespace)


########################################################################
########################################################################


def jinja2_expand_str(s, argdict={}):
    """
    returns the template-completed query from a string
    """
    # Treat the string as a Jinja2 template.
    jenv = _setup_jinja_env()
    templ = jenv.from_string(s)
    try:
        return templ.render(**argdict)
    except jinja2.exceptions.UndefinedError, exc_val:
        raise TemplateError, str(exc_val)


class TemplateError(Exception):
    pass


def _setup_jinja_env():

    def jinja_filter_join(seq, sep=", "):
        """
        jinja filter to stringify elements in a sequence and join on a separator
        """
        return sep.join(str(x) for x in seq)

    def jinja_filter_qjoin(seq, sep=", "):
        """
        jinja filter to string-quote elements in a sequence and join on a separator
        """
        return sep.join(repr(str(x)) for x in seq)

    def jinja_filter_attr_join(seq, attr, sep=", "):
        """
        jinja filter to stringify elements in a sequence and join on a separator
        """
        return sep.join(str(x[attr]) for x in seq)

    def jinja_filter_adapt(v):
        """
        jinja filter to adapt Python object to SQL value (e.g. properly SQL quote a string)
        """
        return adapt(v).getquoted()


    jenv = jinja2.Environment(extensions=['jinja2.ext.do'])
    jenv.undefined = jinja2.StrictUndefined
    jenv.filters["qjoin"] = jinja_filter_qjoin
    jenv.filters["join"] = jinja_filter_join
    jenv.filters["attr_join"] = jinja_filter_attr_join
    if adapt is not None:
        jenv.filters["adapt"] = jinja_filter_adapt
        

    return jenv


