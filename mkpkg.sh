#!/bin/bash

function fmkpkg {
	VERSION=`cat $1/default/app.conf | grep ^version | sed -e "s/version[[:space:]]*=[[:space:]]*//"`
	rm -f $1-$VERSION.tar.gz
	tar czXf exclusion-file $1-$VERSION.tar.gz $1
}

fmkpkg GoogleAnalyticsBQ
fmkpkg dto-analytics
