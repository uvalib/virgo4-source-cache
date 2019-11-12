#!/usr/bin/env bash
#
# run any necessary migrations
#

if [ -z "$DB_HOST" ]; then
   echo "ERROR: DB_HOST is not defined"
   exit 1
fi

if [ -z "$DB_PORT" ]; then
   echo "ERROR: DB_PORT is not defined"
   exit 1
fi

if [ -z "$DB_USER" ]; then
   echo "ERROR: DB_USER is not defined"
   exit 1
fi

if [ -z "$DB_PASSWD" ]; then
   echo "ERROR: DB_PASSWD is not defined"
   exit 1
fi

if [ -z "$DB_NAME" ]; then
   echo "ERROR: DB_NAME is not defined"
   exit 1
fi

# run the migrations
bin/migrate -path db/migrations -verbose -database postgres://$DB_USER:$DB_PASSWD@$DB_HOST:$DB_PORT/$DB_NAME up

# return the status
exit $?

#
# end of file
#
