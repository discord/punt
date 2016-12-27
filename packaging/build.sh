#!/bin/bash

if [ -z $VERSION ]; then
  echo "Please set the VERSION env variable"
  exit 1
fi

mkdir -p build
pushd build

# Create build folders for package
mkdir -p usr/bin

# Build punt
go build ../..

# Copy files in place
mv punt usr/bin/

popd

fpm \
  -s dir \
  -t deb \
  -v $VERSION \
  -n punt \
  -m "Andrei Zbikowski <az@discordapp.com>" \
  --url "https://github.com/hammerandchisel/punt" \
  --deb-upstart upstart/punt.conf \
  build/=/

rm -r build
