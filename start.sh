#!/bin/bash

unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
    exename=erl
else
    #exename=erl.exe
    exename='start //MAX werl.exe'
fi
$exename -name erloci@127.0.0.1 -pa deps/*/ebin -pa ebin -eval "oci_port:run(1,1)."
#$exename -name erloci@127.0.0.1 -pa deps/*/ebin -pa ebin
