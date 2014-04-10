#!/bin/bash
erl -pa deps/*/ebin ebin -s hackney -s hackney_test \
  -hackney_test aws_key '<<"'$AWS_KEY'">>' \
  -hackney_test aws_secret '<<"'$AWS_SECRET'">>'
