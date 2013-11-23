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
package org.springframework.cassandra.core;

/**
 * <b>This is the primary class in core for binding many values to a Cassandra PreparedStatement.</b>
 * 
 * <p>
 * This factory will hold a cached version of the PreparedStatement, and bind many value sets to that statement
 * returning a BoundStatement that can be passed to a Session.execute(Query).
 * </p>
 * 
 * @author David Webb
 * 
 */
public class BoundStatementFactory {

}
