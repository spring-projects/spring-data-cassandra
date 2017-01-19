#!/usr/bin/env bash

if [ -z ${CASSANDRA_VERSION+x} ]; then
    CASSANDRA_VERSION=3.0.7
fi

if [[ ! -d download ]] ; then
    mkdir -p download
fi

FILENAME="apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"

echo "[INFO] Downloading ${FILENAME}"
if [[ ! -f download/${FILENAME} ]] ; then
    mkdir -p download
    wget https://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}/${FILENAME} -O download/${FILENAME}
    if [[ $? != 0 ]] ; then
        echo "[ERROR] Download failed"
        exit 1
    fi
fi


if [[ ! -d work ]] ; then
    mkdir -p work
fi

BASENAME=apache-cassandra-${CASSANDRA_VERSION}
if [[ ! -d work/${BASENAME} ]] ; then

    echo "[INFO] Extracting ${FILENAME}"
    mkdir -p work/${BASENAME}
    cd work

    tar xzf ../download/${FILENAME}
    if [[ $? != 0 ]] ; then
        echo "[ERROR] Extraction failed"
        exit 1
    fi
    cd ..
fi

cd work/${BASENAME}

echo "[INFO] Cleaning data directory"
rm -Rf data
mkdir -p data

echo "[INFO] Starting Apache Cassandra ${CASSANDRA_VERSION}"
export MAX_HEAP_SIZE=1500M
export HEAP_NEWSIZE=300M
bin/cassandra

for start in {1..20}
do
    nc  -w 1 localhost 9042 </dev/null
    if [[ $? == 0 ]] ; then
        echo "[INFO] Cassandra is up and running"
        cd ../..
        exit 0
    fi
    sleep 1
done

echo "[ERROR] Cannot connect to Cassandra"
exit 1

