# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Example usage:
#   docker build -t splinterdb . && docker --rm run splinterdb

# see Dockerfile.build-env
ARG build_env_image=projects.registry.vmware.com/splinterdb/build-env:latest

# see Dockerfile.build-env
ARG run_env_image=projects.registry.vmware.com/splinterdb/run-env:latest

FROM $build_env_image AS build
COPY . /src
RUN make -C /src

FROM $run_env_image
COPY --from=build /src/bin/driver_test /splinterdb/bin/driver_test
COPY --from=build /src/bin/splinterdb.so /splinterdb/bin/splinterdb.so
COPY --from=build /src/test.sh /splinterdb/test.sh

# TODO(gabe): fold this into a make target, once we've sorted out include directories
COPY --from=build /src/src/kvstore.h /src/src/data.h /src/src/platform_public.h /splinterdb/include/

WORKDIR "/splinterdb"
CMD ["/splinterdb/test.sh"]
