# Running with Docker

A `Dockerfile` is supplied in the root of the repository. It's based on
`ubuntu:20.04` and `python3.7`. It installs `GDAL` and `SNAP`.

Note that `-v $PWD/:/code/` is used, with a local directory `/logs`, to
enable logging output for whatever operation runs.

## Docker for Sentinel Preprocessing Pipeline

To install an image named `senprep` with tag `0.1.0`

```bash
sudo docker build -f Dockerfile-senprep -t senprep:0.1.0 --force-rm=true .
```

### run (general form)

To run via docker:

```bash
sudo docker run --rm -it -v $PWD/:/code/ senprep:0.1.0  [command]
```

### Run (list products)

To list products for a certain configuration:

```bash
sudo docker run --rm -it -v $PWD/:/code/ senprep:0.1.0 list --credentials credentials.json  --config ~/code/Sentinel_Preprocess/configurations/Galloway-s256x256-o0x0-20190601to20190701.json
```

To help with debugging, logging is output to a file of the form `logs/<configname>.log`, e.g.  `logs/Cairngorms-s256x256-o0x0-20200501to20200601.json.log`. This can be useful for judging runtime, as well as if any errors occur during operation.

### Run (download products)

To *download*, the docker image needs a directory for data, to be mounted

```bash
sudo docker run --rm -it -v $PWD/data/:/var/satellite-data/ -v $PWD/:/code/  senprep:0.1.0 download --credentials credentials.json --config ~/code/Sentinel_Preprocess/configurations/Galloway-s256x256-o0x0-20190601to20190701.json
```

...example using our servers

```bash
sudo docker run --rm -it  -v /var/satellite-data/:/var/satellite-data/ -v $PWD/:/code/ senprep:0.1.0 download --credentials credentials.json --config ~/code/Sentinel_Preprocess/configurations/Galloway-s256x256-o0x0-20190601to20190701.json
```

## Docker for research (GPU & Jupyter notebook, and senprep pipeline)

To install the gpu+jupyter development environment:

```bash
sudo docker build -f Dockerfile-gpu-jupyter-senprep -t gpu-jupyter-senprep:0.1.0 --force-rm=true .
```

The image can be ran in a very similar way to the above scripts, however
some helper scripts have been created inside the `helpers` folder. These are
`go` programs, so must be compiled (although they should generally already be
installed on our CIDCOm servers).

When run, these will assign the appropriate `uid` and `gid` to the
jupyter environment (allowing tunneling and connection via browser), and
will mount the _current working directory_ to the docker instance. For example
(assuming `docker-jupyter` is installed somewhere on `$PATH`), if
you want `/home/<username>/CODE` to be mounted, you would:

```bash
cd /home/<username>/CODE
docker-jupyter <port>
```

