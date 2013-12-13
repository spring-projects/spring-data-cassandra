/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.keyspace;

/**
 * Class that offers static methods as entry points into the fluent API for building create, drop and alter index
 * specifications. These methods are most convenient when imported statically.
 * 
 * @author Matthew T. Adams
 * @author David Webb
 */
public class IndexOperations {

	/**
	 * Entry point into the {@link CreateIndexSpecification}'s fluent API to create a index. Convenient if imported
	 * statically.
	 */
	public static CreateIndexSpecification createIndex() {
		return new CreateIndexSpecification();
	}

	/**
	 * Entry point into the {@link DropIndexSpecification}'s fluent API to drop a table. Convenient if imported
	 * statically.
	 */
	public static DropIndexSpecification dropIndex() {
		return new DropIndexSpecification();
	}

}