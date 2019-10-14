#
# build the target application
#
FROM golang:1.13 as builder
WORKDIR /build
COPY go.mod go.sum Makefile ./
COPY cmd ./cmd
RUN make linux

#
# build the target container
#
FROM alpine:3.10

# update the packages
RUN apk update && apk upgrade && apk add bash tzdata ca-certificates && rm -rf /var/cache/apk/*

# Create the run user and group
RUN addgroup webservice && adduser webservice -G webservice -D

# set the timezone appropriatly
ENV TZ=UTC
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Specify home 
ENV APP_HOME /virgo4-source-cache
WORKDIR $APP_HOME

# Create necessary directories
RUN mkdir -p $APP_HOME $APP_HOME/bin $APP_HOME/scripts
RUN chown -R webservice $APP_HOME && chgrp -R webservice $APP_HOME

# run command
CMD scripts/entry.sh

# Move in necessary assets
COPY package/data/container_bash_profile /home/webservice/.profile
COPY package/scripts/entry.sh $APP_HOME/scripts/entry.sh
COPY --from=builder /build/bin/virgo4-source-cache.linux $APP_HOME/bin/virgo4-source-cache

# Ensure permissions are correct
RUN chown webservice:webservice /home/webservice/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/virgo4-source-cache && chmod 755 /home/webservice/.profile $APP_HOME/scripts/entry.sh $APP_HOME/bin/virgo4-source-cache

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch $APP_HOME/buildtag.build-$BUILD_TAG || touch $APP_HOME/buildtag.build-0

# Specify the user
USER webservice

#
# end of file
#
