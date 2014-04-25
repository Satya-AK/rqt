
set -ex

export table="dsdk_v0.client_entity"

rqt show-query ex01-jinja2.rqt
rqt show-query ex01-mako.rqt
rqt show-query ex01-mustache.rqt
x=5 y=3 rqt show-query features-mako.rqt


rqt run-query ex01-jinja2.rqt junk.csv



