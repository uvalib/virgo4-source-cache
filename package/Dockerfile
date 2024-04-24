#
# build the target application
#
FROM public.ecr.aws/docker/library/golang:1.21.6-alpine3.19 as builder
RUN apk update && apk upgrade && apk add --no-cache make

WORKDIR /build
COPY go.mod go.sum Makefile ./
COPY cmd ./cmd
RUN make linux

#
# build the target container
#
FROM public.ecr.aws/docker/library/alpine:3.19

# update the packages
RUN apk update && apk upgrade && apk add bash tzdata ca-certificates curl && rm -rf /var/cache/apk/*

# Create the run user and group
RUN addgroup webservice && adduser webservice -G webservice -D

# set the timezone appropriatly
ENV TZ=UTC
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Specify home 
ENV APP_HOME /virgo4-source-cache
WORKDIR $APP_HOME

# Create necessary directories
RUN mkdir -p $APP_HOME $APP_HOME/bin $APP_HOME/scripts $APP_HOME/db/migrations
RUN chown -R webservice $APP_HOME && chgrp -R webservice $APP_HOME

# add the migrate tool
ENV MIGRATE_VERSION v4.17.1
RUN cd $APP_HOME/bin && curl -L https://github.com/golang-migrate/migrate/releases/download/${MIGRATE_VERSION}/migrate.linux-amd64.tar.gz | tar xvz && rm LICENSE README.md

# run command
CMD scripts/entry.sh

# Move in necessary assets
COPY package/data/container_bash_profile /home/webservice/.profile
COPY package/scripts/entry.sh $APP_HOME/scripts/entry.sh
COPY package/scripts/migrate.sh $APP_HOME/scripts/migrate.sh
COPY db/migrations/*.sql $APP_HOME/db/migrations/
COPY --from=builder /build/bin/virgo4-source-cache.linux $APP_HOME/bin/virgo4-source-cache

# Ensure permissions are correct
RUN chown webservice:webservice /home/webservice/.profile $APP_HOME/scripts/entry.sh $APP_HOME/scripts/migrate.sh $APP_HOME/bin/virgo4-source-cache $APP_HOME/bin/migrate && chmod 755 /home/webservice/.profile $APP_HOME/scripts/entry.sh $APP_HOME/scripts/migrate.sh $APP_HOME/bin/virgo4-source-cache $APP_HOME/bin/migrate
RUN chown -R webservice:webservice $APP_HOME/db && chmod -R 755 $APP_HOME/db

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch $APP_HOME/buildtag.build-$BUILD_TAG || touch $APP_HOME/buildtag.build-0

# Specify the user
USER webservice

#
# end of file
#
