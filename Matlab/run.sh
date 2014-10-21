#!/bin/bash
#set -x

function Usage()
{
cat <<-ENDOFMESSAGE

$0 [OPTION] REQ1 REQ2

options:

    -b -branch  branch to use
    -h --help   display this message

ENDOFMESSAGE
    exit 1
}

function Die()
{
    echo "$*"
    exit 1
}

function GetOpts() {
    branch=""
    argv=()
    while [ $# -gt 0 ]
    do
        opt=$1
        shift
        case ${opt} in
            -b|--branch)
                if [ $# -eq 0 -o "${1:0:1}" = "-" ]; then
                    Die "The ${opt} option requires an argument."
                fi
                branch="$1"
                shift
                ;;
            -h|--help)
                Usage;;
            *)
                if [ "${opt:0:1}" = "-" ]; then
                    Die "${opt}: unknown option."
                fi
                argv+=(${opt});;
        esac
    done 
}

GetOpts $*
echo "branch ${branch}"
echo "argv ${argv[@]}"
