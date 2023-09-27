#!/bin/bash

set -e
set -x

# Checks if execution is LAMBDA COMPUTE TYPE
if expr "$AWS_EXECUTION_ENV" : "AWS_LAMBDA_"; then
  if [ -z "${AWS_LAMBDA_RUNTIME_API}" ]; then
    exec aws-lambda-rie python -m awslambdaric "$@"
  else
    exec python -m awslambdaric "$@"
  fi
# assumes execution is ECS COMPUTE TYPE (AWS_ECS_EC2 or AWS_ECS_FARGATE)
else

  # This is defined in setup.cfg
  handle-lambda-request "$@"

fi
