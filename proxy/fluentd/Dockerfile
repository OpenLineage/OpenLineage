FROM fluent/fluentd:v1.16-debian-amd64-1

# Use root account to use apk
USER root

ADD ./ /fluent-openlinege

RUN buildDeps="sudo make gcc g++ libc-dev" \
 && apt-get update \
 && apt-get install -y --no-install-recommends $buildDeps \
 && sudo gem sources --clear-all \
 && SUDO_FORCE_REMOVE=yes \
    apt-get purge -y --auto-remove \
                  -o APT::AutoRemove::RecommendsImportant=false \
                  $buildDeps \
 && rm -rf /var/lib/apt/lists/* \
 && rm -rf /tmp/* /var/tmp/* /usr/lib/ruby/gems/*/cache/*.gem

RUN cd /fluent-openlinege/ \
  bundle install

ADD fluent.conf /fluentd/etc/fluent.conf
ADD entrypoint.sh /bin/entrypoint.sh

USER fluent