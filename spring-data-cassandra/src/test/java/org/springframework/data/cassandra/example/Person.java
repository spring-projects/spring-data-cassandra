/*
 * Copyright 2020-2021 the original author or authors.
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
// tag::file[]
package org.springframework.data.cassandra.example;

import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Table
public class Person {

	@PrimaryKey private final String id;

	private final String name;
	private final int age;

	public Person(String id, String name, int age) {
		this.id = id;
		this.name = name;
		this.age = age;
	}

	public String getId() {
		return id;
	}

	private String getName() {
		return name;
	}

	private int getAge() {
		return age;
	}

	@Override
	public String toString() {
		return String.format("{ @type = %1$s, id = %2$s, name = %3$s, age = %4$d }", getClass().getName(), getId(),
				getName(), getAge());
	}
}
// end::file[]
