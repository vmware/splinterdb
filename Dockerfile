# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

FROM ubuntu:18.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install gawk -y
RUN apt-get install libaio-dev -y
RUN apt-get install libconfig-dev -y
RUN apt-get install ctags -y
RUN apt-get install clang-8 -y
RUN apt-get install python3 -y
RUN apt-get install vim -y
RUN apt-get install -y lldb-3.9
RUN apt install -y libxxhash-dev

RUN mkdir -p /splinterdb
COPY ./ /splinterdb
WORKDIR /splinterdb
RUN chmod +x ./script.sh

RUN make

CMD ["/splinterdb/script.sh"]
