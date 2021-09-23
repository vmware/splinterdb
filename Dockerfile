# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Source for the image
#    projects.registry.vmware.com/splinterdb/splinterdb
#
# When built, this image contains runtime dependencies for SplinterDB,
# the test binary, shared library and header files.
# This image is suitable for integrating SplinterDB into another application.
#
# It does not contain dependencies for building SplinterDB itself from source.
# For that, look at `Dockerfile.build-env`.
#
# Example usage:
# $ docker build -t splinterdb . && docker --rm run splinterdb

# see Dockerfile.build-env
ARG build_env_image=projects.registry.vmware.com/splinterdb/build-env:latest

# see Dockerfile.build-env
ARG run_env_image=projects.registry.vmware.com/splinterdb/run-env:latest

FROM $build_env_image AS build
COPY . /splinterdb-src
ARG compiler=clang
ENV CC=$compiler
ENV LD=$compiler
RUN $compiler --version \
    && make -C /splinterdb-src \
    && mkdir /splinterdb-install \
    && INSTALL_PATH=/splinterdb-install make -C /splinterdb-src install

FROM $run_env_image

# Put the library and headers in the standard location
COPY --from=build /splinterdb-install/lib/* /usr/local/lib/
COPY --from=build /splinterdb-install/include/splinterdb/* /usr/local/include/splinterdb/

# Copy over the test binary and test script
COPY --from=build /splinterdb-src/bin/* /splinterdb/bin/
COPY --from=build /splinterdb-src/test.sh /splinterdb/test.sh
# TODO: Currently driver_test dynamically links against the relative path lib/libsplinterdb.so
# Instead we should link driver_test statically against libsplinterdb.a so that this hack isn't necessary
RUN mkdir /splinterdb/lib && ln -s /usr/local/lib/libsplinterdb.so /splinterdb/lib/libsplinterdb.so

WORKDIR "/splinterdb"
CMD ["/splinterdb/test.sh"]
