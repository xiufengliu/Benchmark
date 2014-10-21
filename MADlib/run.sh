#!/bin/bash
#set -x

# command line parameter parsing fun
usage() { echo "Usage: $0 -s <scale factors> -d <databases to test> -p <directory prefix>" 1>&2; exit 1; }

while getopts ":s:d:p:" o; do
    case "${o}" in
        s)
            s=${OPTARG}
            ;;
        d)
            d=${OPTARG}
            ;;
        p)
            p=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
echo $((OPTIND-1))
shift $((OPTIND-1))

if [ -z "${p}" ] ; then
    echo "-p is required. Example: -p /tmp/ehannes/" 1>&2;
    usage
fi
