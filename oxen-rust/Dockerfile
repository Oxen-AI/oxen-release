FROM rust:1.62

WORKDIR /usr/src/oxen-server

USER root
RUN apt-get update && apt-get install --assume-yes apt-utils
RUN apt-get install -y libclang-dev

COPY . .
RUN cargo install --path .

ENV SYNC_DIR=/var/oxen/data
EXPOSE 3000
CMD ["oxen-server", "start"]
