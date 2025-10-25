### Docker Images for SCR

The Dockerfiles, resulting docker images, and `docker-run-checks.sh`
script contained herein are used as part of the strategy for CI testing
of SCR.

Docker is used under CI to speed up deployment of an
environment with correct build dependencies and to keep a docker
image deployed at `libscr/scr` DockerHub with latest master build
(`libscr/scr:latest`) and tagged builds (`scr/SCR:v<tag>`),
which can be used by other projects to build against the latest
or a tagged version of SCR.

#### libscr/scr Docker Images

The Dockerfiles `fedora40/Dockerfile`,
`el9/Dockerfile`, and `alpine/Dockerfile` describe the images built
under `libscr/scr:fedora40`, `libscr/scr:el9`, and `libscr/scr:alpine`
respectively, and include the base dependencies required to build SCR.
The Dockerfiles should be kept up to date for a single point of management.

#### Multiarch Images

Building the images for linux/amd64 and linux/arm64 requires the
Docker buildx extensions, see

 https://www.docker.com/blog/multi-arch-build-and-images-the-simple-way/

and run
```
$  docker buildx build --push --platform=linux/arm64,linux/amd64 --tag libscr/scr:el9 -f src/test/docker/el9/Dockerfile .
```

to build and push images to docker hub.

#### Local Testing

Developers can test the docker images themselves. If new dependencies are needed,
they can update the `$image` Dockerfiles manually.
To create a local Docker image, run the command:

```
docker build -t libscr/scr:$image src/test/docker/$image
```

To test the locally created image, run:

```
src/test/docker/docker-run-checks.sh -i $image [options] -- [arguments]
```
