FROM adoptopenjdk/openjdk8:latest

RUN set -eux; \
	CASSANDRA_URL='https://archive.apache.org/dist/cassandra/3.11.10/apache-cassandra-3.11.10-bin.tar.gz'; \
	sed -i -e 's/http/https/g' /etc/apt/sources.list ; \
	curl -LfsSo /tmp/cassandra.tar.gz ${CASSANDRA_URL}; \
		mkdir -p /opt/cassandra /opt/cassandra/data /opt/cassandra/logs; \
		cd /opt/cassandra; \
		tar -xf /tmp/cassandra.tar.gz --strip-components=1; \
		rm -rf /tmp/cassandra.tar.gz; \
		chmod -R a+rwx /opt/cassandra; \
		useradd -d /home/jenkins-docker -m -u 1001 -U jenkins-docker;

ENV MAX_HEAP_SIZE=1500M
ENV HEAP_NEWSIZE=300M
