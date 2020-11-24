#!/bin/bash

sudo apt-get install -y \
   libboost-all-dev \
   python3 \
   python3-pip \
   python3-setuptools \
   ruby-full \
   cmake \
   lcov

sudo apt-get install lcov
sudo gem install coveralls-lcov
go get -u github.com/jandelgado/gcov2lcov

pip3 install --user dataclasses-json Jinja2 importlib_resources pluginbase
