# Installation

## Oxen CLI

The Oxen client mirrors the git command line interface can be installed via [homebrew](https://brew.sh/) or by downloading the debian package on linux.

### Mac

```bash
$ brew tap Oxen-AI/oxen
$ brew install oxen
```

### Ubuntu Latest

```bash
$ wget https://github.com/Oxen-AI/oxen-release/releases/download/v0.4.8/oxen-ubuntu-latest-0.4.8.deb
$ sudo dpkg -i oxen-ubuntu-latest-0.4.8.deb
```

### Ubuntu 20.04

```bash
$ wget https://github.com/Oxen-AI/oxen-release/releases/download/v0.4.8/oxen-ubuntu-20.04-0.4.8.deb
$ sudo dpkg -i oxen-ubuntu-20.04-0.4.8.deb
```

## Oxen Server

The Oxen server binary can be deployed where ever you want to store and backup your data. It is an HTTP server that the client communicates with to enable collaboration.

### Mac

```bash
$ brew tap Oxen-AI/oxen-server
$ brew install oxen-server
```

### Docker

```bash
$ wget https://github.com/Oxen-AI/oxen-release/releases/download/v0.4.8/oxen-server-docker-0.4.8.tar
$ docker load < oxen-server-docker-0.4.8.tar
$ docker run -d -v /var/oxen/data:/var/oxen/data -p 80:3001 oxen/oxen-server:latest
```

### Ubuntu Latest

```bash
$ wget https://github.com/Oxen-AI/oxen-release/releases/download/v0.4.8/oxen-server-ubuntu-latest-0.4.8.deb
$ sudo dpkg -i oxen-server-ubuntu-latest-0.4.8.deb
```

To get up and running using the client and server, you can follow the [getting started docs](README.md).
