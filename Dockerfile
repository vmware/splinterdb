# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Source for the image
#    projects.registry.vmware.com/splinterdb/splinterdb
#
# When built, this image contains runtime dependencies for SplinterDB,
# the test binaries, splinterdb-cli binary, shared library and header files.
# These should help with integrating SplinterDB into another application.
#
# It does not contain dependencies for building SplinterDB itself from source.
# For that, look at `Dockerfile.build-env`.
#
# To build the image from a local source checkout:
# $ docker build -t splinterdb .
# To then run the splinterdb-cli within the build image:
# $ docker run --rm splinterdb bin/splinterdb-cli --help
# or to run tests:
# $ docker run --rm splinterdb ./test.sh

# see Dockerfile.build-env
ARG build_env_image=projects.registry.vmware.com/splinterdb/build-env:latest

# see Dockerfile.run-env
ARG run_env_image=projects.registry.vmware.com/splinterdb/run-env:latest


FROM $build_env_image AS build
COPY . /splinterdb-src
ARG compiler=clang
ENV CC=$compiler
ENV LD=$compiler
ARG with_rust=false
ENV WITH_RUST=$with_rust
RUN $compiler --version \
    && make -C /splinterdb-src \
    && mkdir /splinterdb-install \
    && INSTALL_PATH=/splinterdb-install make -C /splinterdb-src install

FROM $run_env_image

# Put the library and headers in the standard location
COPY --from=build /splinterdb-install/lib/* /usr/local/lib/
COPY --from=build /splinterdb-install/include/splinterdb/* /usr/local/include/splinterdb/

# Copy over the test binaries under bin/ (recursively) and the test script
COPY --from=build /splinterdb-src/bin/ /splinterdb/bin/
COPY --from=build /splinterdb-src/test.sh /splinterdb/test.sh
# TODO: Currently driver_test dynamically links against the relative path lib/libsplinterdb.so
# Instead we should link driver_test statically against libsplinterdb.a so that this hack isn't necessary
RUN mkdir /splinterdb/lib && ln -s /usr/local/lib/libsplinterdb.so /splinterdb/lib/libsplinterdb.so

WORKDIR "/splinterdb"
