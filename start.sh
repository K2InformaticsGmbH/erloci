#!/bin/bash

unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
    exename=erl
elif [[ "$unamestr" == 'Darwin' ]]; then
    exename=erl
else
    #exename=erl.exe
    exename='start //MAX werl.exe'
fi
$exename -pa deps/*/ebin -pa ebin -eval "oci_test:run(2,1000)."
#$exename -name erloci@127.0.0.1 -pa deps/*/ebin -pa ebin
