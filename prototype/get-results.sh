#!/bin/sh

rm -rf $1
hadoop fs -get /users/mschatz/$1 $1
