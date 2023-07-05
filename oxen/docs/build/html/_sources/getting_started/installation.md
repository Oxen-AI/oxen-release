# Installation

## CLI Install

The Oxen client can be installed via [homebrew](https://brew.sh/) or by downloading the relevant binaries for Linux or Windows.

You can find the source code for the client [here](https://github.com/Oxen-AI/Oxen) and can also build for source for your platform. The continuous integration pipeline will build binaries for each release in [this repository](https://github.com/Oxen-AI/Oxen).

### Mac

```bash
brew tap Oxen-AI/oxen
```

```bash
brew install oxen
```

### Ubuntu Latest

```bash
<<<<<<< HEAD
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.6.6/oxen-ubuntu-latest-0.6.6.deb
```

```bash
sudo dpkg -i oxen-ubuntu-latest-0.6.6.deb
=======
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.7.0/oxen-ubuntu-latest-0.7.0.deb
```

```bash
sudo dpkg -i oxen-ubuntu-latest-0.7.0.deb
>>>>>>> d8a7ea8 (update docs for cli 0.7.0)
```

### Ubuntu 20.04

```bash
<<<<<<< HEAD
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.6.6/oxen-ubuntu-20.04-0.6.6.deb
```

```bash
sudo dpkg -i oxen-ubuntu-20.04-0.6.6.deb
=======
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.7.0/oxen-ubuntu-20.04-0.7.0.deb
```

```bash
sudo dpkg -i oxen-ubuntu-20.04-0.7.0.deb
>>>>>>> d8a7ea8 (update docs for cli 0.7.0)
```

### Windows

```bash
<<<<<<< HEAD
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.6.6/oxen.exe
=======
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.7.0/oxen.exe
>>>>>>> d8a7ea8 (update docs for cli 0.7.0)
```

## Server Install

The Oxen server binary can be deployed where ever you want to store and backup your data. It is an HTTP server that the client communicates with to enable collaboration.

### Mac

```bash
brew tap Oxen-AI/oxen-server
```

```bash
brew install oxen-server
```

### Docker

```bash
<<<<<<< HEAD
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.6.6/oxen-server-docker-0.6.6.tar
```

```bash
docker load < oxen-server-docker-0.6.6.tar
=======
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.7.0/oxen-server-docker-0.7.0.tar
```

```bash
docker load < oxen-server-docker-0.7.0.tar
>>>>>>> d8a7ea8 (update docs for cli 0.7.0)
```

```bash
docker run -d -v /var/oxen/data:/var/oxen/data -p 80:3001 oxen/oxen-server:latest
```

### Ubuntu Latest

```bash
<<<<<<< HEAD
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.6.6/oxen-server-ubuntu-latest-0.6.6.deb
```

```bash
sudo dpkg -i oxen-server-ubuntu-latest-0.6.6.deb
=======
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.7.0/oxen-server-ubuntu-latest-0.7.0.deb
```

```bash
sudo dpkg -i oxen-server-ubuntu-latest-0.7.0.deb
>>>>>>> d8a7ea8 (update docs for cli 0.7.0)
```

### Ubuntu 20.04

```bash
<<<<<<< HEAD
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.6.6/oxen-server-ubuntu-20.04-0.6.6.deb
```

```bash
sudo dpkg -i oxen-server-ubuntu-20.04-0.6.6.deb
=======
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.7.0/oxen-server-ubuntu-20.04-0.7.0.deb
```

```bash
sudo dpkg -i oxen-server-ubuntu-20.04-0.7.0.deb
>>>>>>> d8a7ea8 (update docs for cli 0.7.0)
```

### Windows

```bash
<<<<<<< HEAD
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.6.6/oxen-server.exe
=======
wget https://github.com/Oxen-AI/Oxen/releases/download/v0.7.0/oxen-server.exe
>>>>>>> d8a7ea8 (update docs for cli 0.7.0)
```

To get up and running using the client and server, you can follow the [getting started docs](https://github.com/Oxen-AI/oxen-release).
