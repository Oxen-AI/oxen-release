# Installation

## Oxen CLI

The Oxen client can be installed via [homebrew](https://brew.sh/) or by downloading the relevant binaries for Linux or Windows.

You can find the source code for the client [here](https://github.com/Oxen-AI/Oxen) and can also build for source for your platform. The continuous integration pipeline will build binaries for each release in [this repository]((https://github.com/Oxen-AI/Oxen)).

### Mac

```bash
$ brew tap Oxen-AI/oxen
$ brew install oxen
```

### Ubuntu Latest

```bash
$ wget https://github.com/Oxen-AI/Oxen/releases/download/0.5.2/oxen-ubuntu-latest-0.5.2.deb
$ sudo dpkg -i oxen-ubuntu-latest-0.5.2.deb
```

### Ubuntu 20.04

```bash
$ wget https://github.com/Oxen-AI/Oxen/releases/download/0.5.2/oxen-ubuntu-20.04-0.5.2.deb
$ sudo dpkg -i oxen-ubuntu-20.04-0.5.2.deb
```

### Windows

```bash
$ wget https://github.com/Oxen-AI/Oxen/releases/download/0.5.2/oxen.exe
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
$ wget https://github.com/Oxen-AI/Oxen/releases/download/0.5.2/oxen-server-docker-0.5.2.tar
$ docker load < oxen-server-docker-0.5.2.tar
$ docker run -d -v /var/oxen/data:/var/oxen/data -p 80:3001 oxen/oxen-server:latest
```

### Ubuntu Latest

```bash
$ wget https://github.com/Oxen-AI/Oxen/releases/download/0.5.2/oxen-server-ubuntu-latest-0.5.2.deb
$ sudo dpkg -i oxen-server-ubuntu-latest-0.5.2.deb
```

### Ubuntu 20.04

```bash
$ wget https://github.com/Oxen-AI/Oxen/releases/download/0.5.2/oxen-server-ubuntu-20.04-0.5.2.deb
$ sudo dpkg -i oxen-server-ubuntu-20.04-0.5.2.deb
```

### Windows

```bash
$ wget https://github.com/Oxen-AI/Oxen/releases/download/0.5.2/oxen-server.exe
```

To get up and running using the client and server, you can follow the [getting started docs](README.md).
