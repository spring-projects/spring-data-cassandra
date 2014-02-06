package org.springframework.cassandra.core.util;

import java.util.ArrayList;
import java.util.List;

public class CollectionUtils {

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
}
