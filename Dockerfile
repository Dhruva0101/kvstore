# Multi-stage build: compile with the full toolchain, ship only the binaries.
FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake make ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt .
COPY include include
COPY src src
COPY tests tests
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DKV_BUILD_TESTS=OFF \
    && cmake --build build -j --target kvserver kvbench kvcli

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/kvserver /src/build/kvbench /src/build/kvcli /usr/local/bin/
WORKDIR /data
EXPOSE 7000 8000
ENTRYPOINT ["kvserver"]
