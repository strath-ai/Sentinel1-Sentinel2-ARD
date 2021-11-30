# Sentinel1-Sentinel2-ARD

Creates analysis read data (ARD) from sentinel data. Creates multi-modal and/or multi-temporal Sentinel-1 and Sentinel-2 ARD.

## Running the pipeline

A `Dockerfile` is supplied in the root of the repository. It's based on `ubuntu:20.04` and `python3.7`. It installs `GDAL` and `SNAP`, among other python requirements.

From the root of the repository, the docker image can be built using:

```bash
docker build --network=host -f Dockerfile-senprep -t senprep:latest --force-rm=True .
```

...which will create an image tagged `senprep:latest`. (You can also install via `make senprep`, which simply runs the above command).

The provided script in `helpers/docker-snap` allows you to run the command in various ways (download only, download and process, process only). If ran without any arguments, it will output help documentation.

To run the pipeline directly, you must provide two mounts to the docker image: 

1.  where the data will be exported
    -   ex: to store in `/temp/satellite`, use `-v /temp/satellite:/var/satellite-data/`
2.  the current working directory (for loading configurations etc).
    -   `-v $PWD/:/code/`


```bash
sudo docker run --rm -it -v /temp/satellite:/var/satellite-data/ -v $PWD/:/code/ senprep:latest download_and_snap --credentials credentials.json --config configuration.json
```
