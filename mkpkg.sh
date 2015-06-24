#!/bin/bash

VERSION=`cat GoogleAnalyticsBQ/default/app.conf | grep ^version | sed -e "s/version[[:space:]]*=[[:space:]]*//"`
rm -f GoogleAnalyticsBQ-$VERSION.tar.gz
tar czXf exclusion-file GoogleAnalyticsBQ-$VERSION.tar.gz GoogleAnalyticsBQ
