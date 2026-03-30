#!/usr/bin/env bash
set -e
psql -U postgres -c "CREATE DATABASE wruntime_example;"
psql -U postgres -c "CREATE DATABASE wruntime_test;"
