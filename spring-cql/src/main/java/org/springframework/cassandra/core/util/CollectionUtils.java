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

package org.springframework.cassandra.core.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class CollectionUtils extends org.springframework.util.CollectionUtils {

	public static Object[] toArray(Iterable<?> iterable) {
		return toList(iterable).toArray();
	}

	public static <T> List<T> toList(T... elements) {
		List<T> list = Collections.emptyList();

		if (elements != null) {
			list = new ArrayList<T>(elements.length);
			Collections.addAll(list, elements);
		}

		return list;
	}

	public static <T> List<T> toList(Iterable<T> iterable) {
		if (!(iterable instanceof List)) {
			List<T> list = new ArrayList<T>();

			if (iterable != null) {
				for (T element : iterable) {
					list.add(element);
				}
			}

			iterable = list;
		}

		return (List<T>) iterable;
	}

}
