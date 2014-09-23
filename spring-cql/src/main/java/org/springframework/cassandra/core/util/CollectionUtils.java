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
import java.util.List;

public class CollectionUtils {

	@SuppressWarnings("unchecked")
	public static <T> T[] toArray(Iterable<T> i) {
		return (T[]) toList(i).toArray();
	}

	public static <T> List<T> toList(Iterable<T> i) {

		List<T> list = null;
		if (i instanceof List) {
			list = (List<T>) i;
		} else {
			list = new ArrayList<T>();
			for (T t : i) {
				list.add(t);
			}
		}

		return list;
	}

	public static List<?> toList(Object thing) {
		List<Object> list = new ArrayList<Object>();
		list.add(thing);
		return list;
	}

	public static List<?> toList(Object thing1, Object thing2) {
		List<Object> list = new ArrayList<Object>();
		list.add(thing1);
		list.add(thing2);
		return list;
	}

	public static List<?> toList(Object thing1, Object thing2, Object thing3) {
		List<Object> list = new ArrayList<Object>();
		list.add(thing1);
		list.add(thing2);
		list.add(thing3);
		return list;
	}

	public static List<?> toList(Object thing1, Object thing2, Object thing3, Object... rest) {
		List<Object> list = new ArrayList<Object>();
		list.add(thing1);
		list.add(thing2);
		list.add(thing3);
		if (rest != null) {
			for (Object thing : rest) {
				list.add(thing);
			}
		}
		return list;
	}
}
