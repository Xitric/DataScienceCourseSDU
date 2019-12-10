#!/bin/bash

flume-ng agent -c conf -f conf/flume.conf -Dflume.root.logger=INFO,console -n SanFranciscoAgent