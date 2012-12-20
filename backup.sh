#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "usage: $0 db_root backup_root [nsmhdw]"
    echo "n shhdw as -atime in find"
    exit 1
fi

DBROOT=$1
BACKUPROOT=$2
MTIME=false
if [ "$#" -gt 2 ]; then
    MTIME=true
    INTERVAL=$3
fi

DATESTAMP=`date +%Y%m%d_%H%M`
BACKUPFILE=autocomplete.$DATESTAMP.$INTERVAL.tgz

mkdir -p $BACKUPROOT

if $MTIME; then
    find $DBROOT -mtime $INTERVAL | tar -cjvf $BACKUPROOT/$BACKUPFILE
else
    tar -cjvf $BACKUPROOT/$BACKUPFILE
fi

