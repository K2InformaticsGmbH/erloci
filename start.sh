#!/bin/bash

# Copyright 2012 K2Informatics GmbH, Root Längenbold, Switzerland
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
    exename=erl
elif [[ "$unamestr" == 'Darwin' ]]; then
    exename=erl
else
    #exename=erl.exe
    exename='start //MAX werl.exe'
fi
case "$1" in
    test)
        $exename -pa ebin -eval "oci_test:start(20,10000)."
        ;;
    *)
        echo "To run throughput tests use 'test' option"
        $exename -pa ebin -s erloci
        ;;
esac
