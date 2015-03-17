package org.springframework.data.cassandra.test.integration.mapping.udt;

import org.springframework.data.cassandra.mapping.UserDefinedType;

@UserDefinedType
public class Phone {
	
	public String number;

}
