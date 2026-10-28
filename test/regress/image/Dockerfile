# PostgreSQL built from source with its regression suite. The same image runs
# the server and the pg_regress client, so the paths that tests hand to the
# server (regress.so, data files, results/) exist on both sides.
ARG DEBIAN_IMAGE=debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3

FROM ${DEBIAN_IMAGE} AS build
ARG PG_VERSION=17.11
ARG PG_SHA256=dd27f2b3c59e73ed14aa3324901242bf69a032a6347805f274e6260322d42979
RUN apt-get update \
 && apt-get install -y --no-install-recommends bison bzip2 ca-certificates curl flex gcc libc6-dev make perl \
 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSLo /tmp/postgresql.tar.bz2 "https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.bz2" \
 && echo "${PG_SHA256}  /tmp/postgresql.tar.bz2" | sha256sum -c - \
 && mkdir /src && tar -xjf /tmp/postgresql.tar.bz2 -C /src --strip-components=1 && rm /tmp/postgresql.tar.bz2
WORKDIR /src
RUN ./configure --prefix=/usr/local/pgsql --without-icu --without-readline --without-zlib \
 && make -j"$(nproc)" \
 && make install \
 && make -C src/test/regress -j"$(nproc)" \
 && find src/test/regress -name '*.o' -delete

FROM ${DEBIAN_IMAGE}
COPY --from=build /usr/local/pgsql /usr/local/pgsql
COPY --from=build /src/src/test/regress /usr/local/pgsql/regress
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN useradd --create-home postgres \
 && mkdir -p /var/lib/postgresql/data /usr/local/pgsql/regress/results \
 && chown -R postgres /var/lib/postgresql /usr/local/pgsql/regress
ENV PATH=/usr/local/pgsql/bin:$PATH PGDATA=/var/lib/postgresql/data
USER postgres
EXPOSE 5432
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["postgres"]
