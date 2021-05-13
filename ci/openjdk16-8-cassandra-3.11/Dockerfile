FROM adoptopenjdk/openjdk16:latest

RUN set -eux; \
	CASSANDRA_URL='https://www-us.apache.org/dist/cassandra/3.11.9/apache-cassandra-3.11.9-bin.tar.gz'; \
	curl -LfsSo /tmp/cassandra.tar.gz ${CASSANDRA_URL}; \
		mkdir -p /opt/cassandra /opt/cassandra/data /opt/cassandra/logs; \
		cd /opt/cassandra; \
		tar -xf /tmp/cassandra.tar.gz --strip-components=1; \
		rm -rf /tmp/cassandra.tar.gz; \
		chmod -R a+rwx /opt/cassandra; \
		useradd -d /home/jenkins-docker -m -u 1001 -U jenkins-docker;

RUN set -eux; \
	BINARY_URL='https://github.com/AdoptOpenJDK/openjdk8-binaries/releases/download/jdk8u275-b01/OpenJDK8U-jdk_x64_linux_hotspot_8u275b01.tar.gz'; \
	curl -LfsSo /tmp/openjdk.tar.gz ${BINARY_URL}; \
		mkdir -p /opt/java/openjdk8; \
		cd /opt/java/openjdk8; \
		tar -xf /tmp/openjdk.tar.gz --strip-components=1; \
		rm -rf /tmp/openjdk.tar.gz;

ENV PATH="/opt/java/openjdk8/bin:$PATH"
ENV MAX_HEAP_SIZE=1500M
ENV HEAP_NEWSIZE=300M
