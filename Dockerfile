# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

# Example usage:
# docker build -t splinterdb . && docker --rm run --cap-add=IPC_LOCK splinterdb

FROM library/ubuntu:20.04 AS build-env
RUN /bin/bash -c ' \
set -euo pipefail; \
export DEBIAN_FRONTEND=noninteractive; \
apt update -y; \
apt install -y make libaio-dev libconfig-dev clang-8 libxxhash-dev libcap2-bin; \
apt clean;'

ENV CC clang-8
ENV LD clang-8

FROM build-env AS build-artifact
COPY . /src
RUN make -C /src

FROM library/ubuntu:20.04 AS runner
RUN /bin/bash -c ' \
set -euo pipefail; \
export DEBIAN_FRONTEND=noninteractive; \
apt update -y; \
apt install -y libaio1 libxxhash0; \
apt clean;'
COPY --from=build-artifact /src/bin/driver_test /splinterdb/bin/driver_test
COPY --from=build-artifact /src/bin/splinterdb.so /splinterdb/bin/splinterdb.so
COPY --from=build-artifact /src/test.sh /splinterdb/test.sh
WORKDIR "/splinterdb"
CMD ["/splinterdb/test.sh"]
