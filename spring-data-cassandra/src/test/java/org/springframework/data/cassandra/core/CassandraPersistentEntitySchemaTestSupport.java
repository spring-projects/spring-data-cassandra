/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.util.List;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.UserDefinedType;

/**
 * Support class for schema creation/drop tests.
 *
 * @author Mark Paluch
 */
abstract class CassandraPersistentEntitySchemaTestSupport {

	@UserDefinedType
	static class UniverseType {
		String name;
	}

	@UserDefinedType
	static class MoonType {
		UniverseType universeType;
	}

	@UserDefinedType
	static class PlanetType {

		Set<MoonType> moons;
		UniverseType universeType;
	}

	@UserDefinedType
	private static class AstronautType {
		String name;
	}

	@UserDefinedType
	static class SpaceAgencyType {
		List<AstronautType> astronauts;
	}

	@Table
	static class IndexedEntity {

		@Id String id;
		@Indexed String firstName;
	}

	@Table
	static class Person {
		@Id String id;
	}
}
