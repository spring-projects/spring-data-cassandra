/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.keyspace;

/**
 * Builder class to support the construction of keyspace specifications that have columns. This class can also be used
 * as a standalone {@link KeyspaceDescriptor}, independent of {@link CreateKeyspaceSpecification}.
 * 
 * @author John McPeek
 * @author Matthew T. Adams
 */
public class KeyspaceSpecification<T> extends KeyspaceOptionsSpecification<KeyspaceSpecification<T>> implements
		KeyspaceDescriptor {}
