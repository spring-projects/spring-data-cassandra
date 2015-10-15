package org.springframework.data.cassandra.test.integration.mapping.udt;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.springframework.data.cassandra.mapping.UserDefinedType;

@UserDefinedType
public class Phone {
	
	public String number;
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
