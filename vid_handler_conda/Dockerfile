FROM mambaorg/micromamba

LABEL maintainer="Niranjith"

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml

RUN micromamba install -y -n base -f /tmp/environment.yml && micromamba clean --all --yes

WORKDIR /app

COPY --chown=$MAMBA_USER:$MAMBA_USER . .

ARG MAMBA_DOCKERFILE_ACTIVATE=1

CMD [ "python3", "vid_handler.py" ]