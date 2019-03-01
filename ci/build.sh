#!/bin/bash

set -euo pipefail

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

spring_data_cassandra_artifactory=$(pwd)/spring-data-cassandra-artifactory

rm -rf $HOME/.m2/repository/org/springframework/data 2> /dev/null || :

cd spring-data-cassandra-github

./mvnw deploy \
    -Dmaven.test.skip=true \
    -DaltDeploymentRepository=distribution::default::file://${spring_data_cassandra_artifactory}
