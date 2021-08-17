# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Example usage:
#   docker build -t splinterdb . && docker --rm run splinterdb

# see Dockerfile.build-env
ARG build_env_image=projects.registry.vmware.com/splinterdb/build-env:latest

# see Dockerfile.build-env
ARG run_env_image=projects.registry.vmware.com/splinterdb/run-env:latest

FROM $build_env_image AS build
COPY . /splinterdb
ARG compiler=clang
ENV CC=$compiler
ENV LD=$compiler
RUN $compiler --version && make -C /splinterdb

FROM $run_env_image
COPY --from=build /splinterdb/bin/driver_test /splinterdb/bin/driver_test
COPY --from=build /splinterdb/bin/splinterdb.so /splinterdb/bin/splinterdb.so
COPY --from=build /splinterdb/test.sh /splinterdb/test.sh

# TODO(gabe): fold this into a make target, once we've sorted out include directories
COPY --from=build /splinterdb/src/kvstore.h /splinterdb/src/data.h /splinterdb/src/platform_public.h /splinterdb/include/

WORKDIR "/splinterdb"
CMD ["/splinterdb/test.sh"]
