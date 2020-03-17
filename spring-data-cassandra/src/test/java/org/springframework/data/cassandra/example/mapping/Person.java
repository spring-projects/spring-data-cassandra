/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.cassandra.example.mapping;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import org.springframework.data.annotation.Transient;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import com.datastax.oss.driver.api.core.data.UdtValue;

// tag::class[]
@Table("my_person")
public class Person {

	@PrimaryKeyClass
	public static class Key implements Serializable {

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
		private String type;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED)
		private String value;

		@PrimaryKeyColumn(name = "correlated_type", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
		private String correlatedType;

		// other getters/setters omitted
	}

	@PrimaryKey
	private Person.Key key;

	@CassandraType(type = CassandraType.Name.VARINT)
	private Integer ssn;

	@Column("f_name")
	private String firstName;

	@Column
	@Indexed
	private String lastName;

	private Address address;

	@CassandraType(type = CassandraType.Name.UDT, userTypeName = "myusertype")
	private UdtValue usertype;

	private Coordinates coordinates;

	@Transient
	private Integer accountTotal;

	@CassandraType(type = CassandraType.Name.SET, typeArguments = CassandraType.Name.BIGINT)
	private Set<Long> timestamps;

	private Map<@Indexed String, InetAddress> sessions;

	public Person(Integer ssn) {
		this.ssn = ssn;
	}

	public Person.Key getKey() {
		return key;
	}

	// no setter for Id.  (getter is only exposed for some unit testing)

	public Integer getSsn() {
		return ssn;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	// other getters/setters omitted
}
// end::class[]
