# Docker-based development
SplinterDB currently only supports Linux.  If you develop on Mac or Windows (or just want to isolate your SplinterDB work from your host machine), you may use Docker to build SplinterDB from source and/or integrate an application against the SplinterDB library.

> Beware: The SplinterDB maintainers do not use these workflows for daily development, so there may be rough edges.  Please report any issues (or submit pull-requests) and we'll do our best to keep this working.

## Building SplinterDB from source in Docker
Our continuous-integration system publishes (and uses) a container image with all build-time dependencies:
```
docker pull projects.registry.vmware.com/splinterdb/build-env
```

To use it to build SplinterDB from source, change into the directory containing this repository and then do
```shell
$ docker run -it --rm --mount type=bind,source="$PWD",target=/splinterdb \
     projects.registry.vmware.com/splinterdb/build-env /bin/bash
```

> Note: the `build-env` image contains a working build environment, but does not
contain the SplinterDB source code.  That must be mounted into the running
container, e.g. the `--mount` command shown above.

Inside the container is a Linux environment with
[all dependencies for building and testing](../Dockerfile.build-env)
with either GCC or Clang.

For example, from inside the running container:
```shell
docker$ cd /splinterdb
docker$ export CC=clang  # or gcc
docker$ export LD=clang
docker$ make
docker$ make run-tests
docker$ make install
```

## Using pre-built SplinterDB
In addition to the build-environment described above, our continuous integration system also publishes a [minimal Docker image](../Dockerfile) that contains only the build _outputs_ and dependencies for _run-time_ usage of SplinterDB:
```
docker pull projects.registry.vmware.com/splinterdb/splinterdb
```
If you don't need to modify SplinterDB itself, this image should be sufficient for linking the SplinterDB library into another program, without bringing along all the build-time dependencies.

Example usage:
```shell
$ docker run -it --rm projects.registry.vmware.com/splinterdb/splinterdb /bin/bash
```

The container includes:
- runtime dependencies for SplinterDB, including `libaio` and `libxxhash`
- the SplinterDB static and shared libraries: `/usr/local/lib/libsplinterdb.{a,so}`
- header files for SplinterDB's public API: `/usr/local/include/splinterdb/`
- some pre-built test binaries and test scripts: `/splinterdb/bin/...` and `/splinterdb/test.sh`

> Note: the `splinterdb` image does not include tools to build SplinterDB itself
from source.  [See above](#building-splinterdb-in-docker) for that.

Docker-based development is beyond the scope of this doc, but consider
using bind mounts to access source code on your host OS:
```shell
$ docker run -it --rm \
    --mount type=bind,source="$PWD/my-app-src",target=/my-app-src \
    projects.registry.vmware.com/splinterdb/splinterdb /bin/bash
```