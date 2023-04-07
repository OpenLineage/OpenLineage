FROM fluent/fluentd:v1.16-debian-amd64-1

# Use root account to use apk
USER root

RUN buildDeps="sudo make gcc g++ libc-dev" \
 && apt-get update \
 && apt-get install -y --no-install-recommends $buildDeps \
 && apt-get install -y ruby-full \
 && apt-get install -y git \
 && gem install ffi -v '1.15.5' --source 'https://rubygems.org/' \
 && gem install rusty_json_schema \
 && sudo gem sources --clear-all \
 && SUDO_FORCE_REMOVE=yes \
    apt-get purge -y --auto-remove \
                  -o APT::AutoRemove::RecommendsImportant=false \
                  $buildDeps \
 && rm -rf /var/lib/apt/lists/* \
 && rm -rf /tmp/* /var/tmp/* /usr/lib/ruby/gems/*/cache/*.gem

ADD ./entrypoint.sh /bin/entrypoint.sh

USER fluent