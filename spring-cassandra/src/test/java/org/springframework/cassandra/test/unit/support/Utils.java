package org.springframework.cassandra.test.unit.support;

import java.util.UUID;

public class Utils {

	public static String randomKeyspaceName() {
		return "ks" + UUID.randomUUID().toString().replace("-", "");
	}

}
