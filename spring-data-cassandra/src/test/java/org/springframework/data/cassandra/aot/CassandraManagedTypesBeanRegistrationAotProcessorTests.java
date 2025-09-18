/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.cassandra.aot;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.util.TypeCollector;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;

/**
 * Unit tests for {@link CassandraManagedTypesBeanRegistrationAotProcessor}.
 *
 * @author Mark Paluch
 */
class CassandraManagedTypesBeanRegistrationAotProcessorTests {

	@Test // GH-1606
	void shouldFilterUnreachableFieldTypes() {
		assertThat(TypeCollector.inspect(CassandraEntity.class).list()).containsOnly(CassandraEntity.class,
				Reachable.class);
	}

	static class Reachable {

	}

	static class CassandraEntity {

		private CqlIdentifier session;
		private UdtValue udt;
		private Reachable reachable;

		public CassandraEntity(CqlIdentifier session, UdtValue udt) {
			this.session = session;
			this.udt = udt;
		}

		public void setUdt(UdtValue udt) {

		}

	}

}
