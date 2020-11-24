#!/bin/bash

sudo gem install coveralls-lcov
go get -u github.com/jandelgado/gcov2lcov
pip3 install --user dataclasses-json Jinja2 importlib_resources pluginbase
