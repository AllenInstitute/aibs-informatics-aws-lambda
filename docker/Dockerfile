# Pin the Python toolchain used in the builder; must match Lambda runtime major.minor
ARG PYTHON_VERSION=3.11


# Resources:
# - https://gallery.ecr.aws/lambda/python
# - https://docs.aws.amazon.com/lambda/latest/dg/python-image.html
# - https://docs.aws.amazon.com/lambda/latest/dg/images-test.html
# - https://github.com/aws/aws-lambda-python-runtime-interface-client


#####################################
############## STAGE 0 ##############
########### UV BINARIES #############
#####################################
FROM ghcr.io/astral-sh/uv:latest AS uvbin

#####################################
############## STAGE 1 ##############
################ Builder ############
#####################################
FROM public.ecr.aws/lambda/python:${PYTHON_VERSION} AS builder

# Minimal tools for resolving/installing deps from the monorepo
RUN yum update -y \
    && yum install -y \
        git \
        openssh-clients \
    && yum clean all \
    && rm -rf /var/cache/yum

# Bring in uv from the official Astral image (builder only)
COPY --from=uvbin /uv /uvx /usr/local/bin/

# Prepare for private repos if needed
RUN mkdir -p -m 0700 /root/.ssh && ssh-keyscan github.com >> /root/.ssh/known_hosts

# Copy the monorepo so relative path installs work
ENV REPO_ROOT=/asset-input
COPY . ${REPO_ROOT}
WORKDIR ${REPO_ROOT}

# Vendor all Python deps (including awslambdaric and local pkgs) into a folder
# that will become ${LAMBDA_TASK_ROOT} in the runtime image.
ENV LAYER_DIR=/opt/lambda-task
RUN --mount=type=ssh \
    uv export --frozen --no-dev --group docker --no-editable -o requirements-autogen.txt && \
    uv pip install -r requirements-autogen.txt --target ${LAYER_DIR}

# Clean up build-time artifacts that might have been created (defensive)
RUN find ${LAYER_DIR} -type d -name "__pycache__" -prune -exec rm -rf {} +

# Copy entrypoint script alongside the vendored packages
RUN cp ${REPO_ROOT}/docker/docker-entrypoint.sh ${LAYER_DIR}/docker-entrypoint.sh \
    && chmod +x ${LAYER_DIR}/docker-entrypoint.sh

#####################################
############## STAGE 2 ##############
############## Runtime ##############
#####################################
FROM public.ecr.aws/lambda/python:${PYTHON_VERSION}

# Base image is Amazon Linux; install helpful tools (retain previous behavior)
RUN yum update -y \
    && yum install -y \
        curl \
        jq \
        unzip \
        wget \
        aws-cli \
        git \
    && yum clean all \
    && rm -rf /var/cache/yum

# Upgrade AWS CLI to v2
ADD https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip /tmp
RUN unzip /tmp/awscli-exe-linux-x86_64.zip -d /tmp \
    && rm /tmp/awscli-exe-linux-x86_64.zip \
    && /tmp/aws/install

# Copy only the vendored application deps into the Lambda task root
# (No uv, no build tools end up in the final image)
COPY --from=builder /opt/lambda-task ${LAMBDA_TASK_ROOT}

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}:${PYTHONPATH}"
ENV PATH="${LAMBDA_TASK_ROOT}/bin:${PATH}"

ENTRYPOINT [ "bash", "-c", "${LAMBDA_TASK_ROOT}/docker-entrypoint.sh" ]
