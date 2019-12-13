#!/bin/bash

for f in */ ; do
    echo "$f"
    if [ -f "$f" ]; then
        python $f bdist_egg
    fi
done
