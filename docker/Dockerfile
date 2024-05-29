FROM public.ecr.aws/lambda/python:3.11

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

COPY . /asset-input

RUN --mount=type=ssh cd /asset-input \
    && python3 -m pip install --upgrade pip \
    && pip3 install .[lambda] \
        --no-cache-dir \
        --target "${LAMBDA_TASK_ROOT}" \
    && cp docker/docker-entrypoint.sh ${LAMBDA_TASK_ROOT}/docker-entrypoint.sh \
    && chmod +x ${LAMBDA_TASK_ROOT}/docker-entrypoint.sh

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}:${PYTHONPATH}"
ENV PATH="${LAMBDA_TASK_ROOT}/bin:${PATH}"

RUN rm -rf /asset-input

ENTRYPOINT [ "bash", "-c", "${LAMBDA_TASK_ROOT}/docker-entrypoint.sh" ]

