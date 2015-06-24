#!/bin/bash

VERSION=`cat GoogleAnalyticsBQ/default/app.conf | grep ^version | sed -e "s/version[[:space:]]*=[[:space:]]*//"`
tar czXf exclusion-file GoogleAnalyticsBQ-$VERSION.tar.gz GoogleAnalyticsBQ
