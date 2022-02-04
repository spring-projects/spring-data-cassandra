ARG BASE
FROM ${BASE}
# Any ARG statements before FROM are cleared.
ARG CASSANDRA

RUN set -eux; \
	CASSANDRA_URL="https://archive.apache.org/dist/cassandra/${CASSANDRA}/apache-cassandra-${CASSANDRA}-bin.tar.gz"; \
	sed -i -e 's/http/https/g' /etc/apt/sources.list ; \
	curl -LfsSo /tmp/cassandra.tar.gz ${CASSANDRA_URL}; \
		mkdir -p /opt/cassandra /opt/cassandra/data /opt/cassandra/logs; \
		cd /opt/cassandra; \
		tar -xf /tmp/cassandra.tar.gz --strip-components=1; \
		rm -rf /tmp/cassandra.tar.gz; \
		chmod -R a+rwx /opt/cassandra; \
		useradd -d /home/jenkins-docker -m -u 1001 -U jenkins-docker;

RUN set -eux; \
	BINARY_URL='https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u322-b06/OpenJDK8U-jdk_x64_linux_hotspot_8u322b06.tar.gz'; \
	curl -LfsSo /tmp/openjdk.tar.gz ${BINARY_URL}; \
		mkdir -p /opt/java/openjdk8; \
		cd /opt/java/openjdk8; \
		tar -xf /tmp/openjdk.tar.gz --strip-components=1; \
		rm -rf /tmp/openjdk.tar.gz;

ENV PATH="/opt/java/openjdk8/bin:$PATH"
ENV MAX_HEAP_SIZE=1500M
ENV HEAP_NEWSIZE=300M
