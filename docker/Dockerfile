FROM public.ecr.aws/lambda/python:3.9

# Resources:
# - https://gallery.ecr.aws/lambda/python
# - https://docs.aws.amazon.com/lambda/latest/dg/python-image.html
# - https://docs.aws.amazon.com/lambda/latest/dg/images-test.html
# - https://github.com/aws/aws-lambda-python-runtime-interface-client

# update system
RUN yum update -y \
    && yum install -y \
        curl \
        jq \
        unzip \
        wget \
        aws-cli \
        git \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

# Base image has an old aws cli version (v1.x), this will replace that
ADD https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip /tmp
RUN unzip /tmp/awscli-exe-linux-x86_64.zip -d /tmp \
    && rm /tmp/awscli-exe-linux-x86_64.zip \
    && /tmp/aws/install

ENV REPO_ROOT=/asset-input

COPY . ${REPO_ROOT}

RUN --mount=type=ssh export PACKAGE_ROOT=${REPO_ROOT}/aibs-informatics-lambda-functions \
    && cd ${PACKAGE_ROOT} \
    && python3 -m pip install --upgrade pip \
    && pip3 install \
        --no-cache-dir \
        --requirement requirements-docker.txt \
        --target "${LAMBDA_TASK_ROOT}" \
    && cp $PACKAGE_ROOT/docker/docker-entrypoint.sh ${LAMBDA_TASK_ROOT}/docker-entrypoint.sh \
    && chmod +x ${LAMBDA_TASK_ROOT}/docker-entrypoint.sh

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}:${PYTHONPATH}"
ENV PATH="${LAMBDA_TASK_ROOT}/bin:${PATH}"

RUN rm -rf ${REPO_ROOT}

ENTRYPOINT [ "bash", "-c", "${LAMBDA_TASK_ROOT}/docker-entrypoint.sh" ]

