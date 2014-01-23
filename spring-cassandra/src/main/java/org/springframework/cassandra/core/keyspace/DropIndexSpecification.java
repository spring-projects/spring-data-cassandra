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
 * Builder class that supports the construction of <code>DROP INDEX</code> specifications.
 * 
 * @author Matthew T. Adams
 * @author David Webb
 */
public class DropIndexSpecification extends IndexNameSpecification<DropIndexSpecification> {

	// private boolean ifExists;

	/**
	 * Entry point into the {@link DropIndexSpecification}'s fluent API to drop a table. Convenient if imported
	 * statically.
	 */
	public static DropIndexSpecification dropIndex() {
		return new DropIndexSpecification();
	}

	/*
	 * In CQL 3.1 this is supported so we can uncomment the exposure then.
	 * In the meantime, it will always be false and tests will pass.
	 */

	// public DropIndexSpecification ifExists() {
	// return ifExists(true);
	// }
	//
	// public DropIndexSpecification ifExists(boolean ifExists) {
	// this.ifExists = ifExists;
	// return this;
	// }
	//
	// public boolean getIfExists() {
	// return ifExists;
	// }
}
