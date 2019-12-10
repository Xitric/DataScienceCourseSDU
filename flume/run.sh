#!/bin/bash

flume-ng agent -c conf -f conf/flume.conf -Dflume.root.logger=DEBUG,console -n SanFranciscoAgent -DpropertiesImplementation=org.apache.flume.node.EnvVarResolverProperties