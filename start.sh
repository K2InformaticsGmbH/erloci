#!/bin/sh
start //MAX werl.exe -name erloci@127.0.0.1 -pa deps/*/ebin -pa ebin -eval "oci_port:run(1,1)."
