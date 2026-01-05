/*
 * Copyright 2022-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.observability;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.docs.ObservationDocumentation;
import io.micrometer.tracing.docs.EventValue;

/**
 * Cassandra-based implementation of {@link ObservationDocumentation}.
 *
 * @author Mark Paluch
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 * @since 4.0
 */
public enum CassandraObservation implements ObservationDocumentation {

	/**
	 * Create an {@link io.micrometer.observation.Observation} for Cassandra-based queries.
	 */
	CASSANDRA_QUERY_OBSERVATION {

		@Override
		public String getName() {
			return "spring.data.cassandra.query";
		}

		@Override
		public KeyName[] getLowCardinalityKeyNames() {
			return LowCardinalityKeyNames.values();
		}

		@Override
		public KeyName[] getHighCardinalityKeyNames() {
			return HighCardinalityKeyNames.values();
		}

	};

	enum LowCardinalityKeyNames implements KeyName {

		/**
		 * Database system.
		 */
		DATABASE_SYSTEM {
			@Override
			public String asString() {
				return "db.system";
			}
		},

		/**
		 * Network transport.
		 */
		NET_TRANSPORT {
			@Override
			public String asString() {
				return "net.transport";
			}
		},

		/**
		 * Name of the database host.
		 */
		NET_PEER_NAME {
			@Override
			public String asString() {
				return "net.peer.name";
			}
		},

		/**
		 * Logical remote port number.
		 */
		NET_PEER_PORT {
			@Override
			public String asString() {
				return "net.peer.port";
			}
		},

		/**
		 * Cassandra peer address.
		 */
		NET_SOCK_PEER_ADDR {
			@Override
			public String asString() {
				return "net.sock.peer.addr";
			}
		},

		/**
		 * Cassandra peer port.
		 */
		NET_SOCK_PEER_PORT {
			@Override
			public String asString() {
				return "net.sock.peer.port";
			}
		},

		/**
		 * Name of the Cassandra keyspace.
		 */
		KEYSPACE_NAME {
			@Override
			public String asString() {
				return "db.name";
			}
		},

		/**
		 * Cassandra session
		 */
		SESSION_NAME {
			@Override
			public String asString() {
				return "spring.data.cassandra.sessionName";
			}
		},

		/**
		 * The method name
		 */
		METHOD_NAME {
			@Override
			public String asString() {
				return "spring.data.cassandra.methodName";
			}
		},

		/**
		 * The database operation.
		 */
		DB_OPERATION {
			@Override
			public String asString() {
				return "db.operation";
			}
		},

		COORDINATOR {
			@Override
			public String asString() {
				return "db.cassandra.coordinator.id";
			}
		},
		COORDINATOR_DC {
			@Override
			public String asString() {
				return "db.cassandra.coordinator.dc";
			}
		}
	}

	enum HighCardinalityKeyNames implements KeyName {

		/**
		 * A key-value containing Cassandra CQL.
		 */
		DB_STATEMENT {
			@Override
			public String asString() {
				return "db.statement";
			}
		},

		PAGE_SIZE {
			@Override
			public String asString() {
				return "db.cassandra.page_size";
			}
		},
		CONSISTENCY_LEVEL {
			@Override
			public String asString() {
				return "db.cassandra.consistency_level";
			}
		},
		IDEMPOTENCE {
			@Override
			public String asString() {
				return "db.cassandra.idempotence";
			}
		},

		/**
		 * A tag containing error that occurred for the given node.
		 */
		NODE_ERROR_TAG {
			@Override
			public String asString() {
				return "spring.data.cassandra.node[%s].error";
			}
		}

	}

	enum Events implements EventValue {

		/**
		 * Set whenever an error occurred for the given node.
		 */
		NODE_ERROR {
			@Override
			public String getValue() {
				return "cassandra.node.error";
			}
		},

		/**
		 * Set when a success occurred for the session processing.
		 */
		NODE_SUCCESS {
			@Override
			public String getValue() {
				return "cassandra.node.success";
			}
		}

	}
}
