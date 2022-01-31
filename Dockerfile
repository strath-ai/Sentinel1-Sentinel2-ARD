# set base image (host OS)
FROM ubuntu:18.04
ARG DEBIAN_FRONTEND=noninteractive

# Install useful prerequisites
RUN apt-get update && apt-get upgrade -y &&\
    apt-get install -y software-properties-common fonts-dejavu fontconfig &&\
    # Install python and python package management
    apt-get install -y curl python3.7 python3.7-dev python3.7-distutils &&\
    apt-get clean &&\
    # Make python3.7 the default
    update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1 &&\
    # Upgrade pip to latest version
    curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py --force-reinstall && \
    rm get-pip.py &&\
    # Remove apt cache
    rm -rf /var/lib/apt/lists/*

# set the working directory in the container
WORKDIR /code

# Install SNAP / GPT
COPY update-snap-modules.sh .
RUN curl -s https://download.esa.int/step/snap/8.0/installers/esa-snap_sentinel_unix_8_0.sh -o esa-snap.sh &&\
    chmod +x esa-snap.sh &&\
    ./esa-snap.sh -q &&\
    rm esa-snap.sh &&\
    chmod +x update-snap-modules.sh && ./update-snap-modules.sh
    # snap --nosplash --nogui --modules --update-all


# Install GDAL and python
RUN add-apt-repository ppa:ubuntugis/ppa &&\ 
    apt-get update &&\
    apt-get install -y gdal-bin libgdal-dev &&\
    export CPLUS_INCLUDE_PATH=/usr/include/gdal &&\ 
    export C_INCLUDE_PATH=/usr/include/gdal
RUN pip install numpy &&\
    pip install wheel setuptools==58.0 &&\
    pip install GDAL==$(gdal-config --version)

# Install other python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt 
# Add 'gpt' directory to PATH
ENV PATH="/usr/local/snap/bin:${PATH}"

COPY src/ src/

# command to run on container start
# 'bash' entrypoint is for debugging
# ENTRYPOINT ["bash"]
# ...otherwise, run 'senprep', with whatever command the user gives
COPY run/snap /usr/bin/
RUN chmod +x /usr/bin/snap
ENV USERNAME=metaflow
ENTRYPOINT ["/usr/bin/snap"] 
CMD []
