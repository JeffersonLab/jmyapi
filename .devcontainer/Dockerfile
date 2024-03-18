FROM ubuntu:jammy

RUN cd /tmp \
    && apt update \
    && apt install -y curl git openjdk-17-jdk zip unzip \
    && apt clean \
    && curl -o /usr/local/share/ca-certificates/customcert.crt http://pki.jlab.org/JLabCA.crt \
    && update-ca-certificates \
    && curl -O https://raw.githubusercontent.com/devcontainers/features/main/src/docker-outside-of-docker/install.sh \
    && chmod +x /tmp/install.sh \
    && /tmp/install.sh
    # && curl -s "https://get.sdkman.io" | bash \
    # && bash -c sdk install gradle 7.4

ENV TZ=America/New_York