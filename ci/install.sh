#!/bin/bash

sudo gem install coveralls-lcov
go get -u github.com/jandelgado/gcov2lcov
go get -u golang.org/x/lint/golint
pip3 install --user dataclasses-json Jinja2 importlib_resources pluginbase
