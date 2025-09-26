
FROM python:3.13-slim
ENV TZ=Asia/Singapore

# Install dependencies
RUN apt-get update --yes \
    && apt-get upgrade --yes \
    && apt install -y --no-install-recommends \
    wget git nano sudo curl fuse-overlayfs

# configure locale
RUN apt-get update && apt-get install -y locales-all 
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# Install docker from official source to use fuse-overlayfs
# fuse-overlayfs is 5 times faster than vfs (default) 
RUN curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh && rm get-docker.sh
RUN mkdir -p /etc/docker && \
    echo '{\n  "storage-driver": "fuse-overlayfs"\n}' > /etc/docker/daemon.json

# uv is faster when rebuilding the container
RUN python -m pip install -U pip setuptools wheel uv

# # create vscode-server folder for persistence of vs code extensions across reloads
RUN mkdir -p /root/.vscode-server 

# set up git (line ending and user info)
RUN git config --global user.name "Yoki" \
    && git config --global user.email "yotokyos@gmail.com" \
    && git config --global core.autocrlf input \
    && git config --global core.eol lf

# nicer terminal
COPY .devcontainer/.bashrc /root/.bashrc
COPY .devcontainer/.bash_history /root/.bash_history

# Set the entrypoint to run when the container starts
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
