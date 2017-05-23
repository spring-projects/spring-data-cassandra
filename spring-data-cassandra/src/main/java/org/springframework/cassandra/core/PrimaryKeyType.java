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
package org.springframework.cassandra.core;

import java.util.Comparator;

/**
 * Values representing primary key column types. Implements {@link Comparator} in that
 * {@link PrimaryKeyType#PARTITIONED} is ordered before {@link PrimaryKeyType#CLUSTERED}.
 * 
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public enum PrimaryKeyType implements Comparator<PrimaryKeyType> {

	/**
	 * Used for a column that is part of the partition key.
	 */
	PARTITIONED,

	/**
	 * Used for a column that is clustered key.
	 */
	CLUSTERED;

	@Override
	public int compare(PrimaryKeyType l, PrimaryKeyType r) {
		if (l == r) {
			return 0;
		}
		if (l == null) {
			return 1;
		}
		if (r == null) {
			return -1;
		}
		return l == PARTITIONED && r == CLUSTERED ? 1 : -1;
	}
}
