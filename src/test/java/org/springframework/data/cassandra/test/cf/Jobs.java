package org.springframework.data.cassandra.test.cf;

import lombok.Data;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.entitystore.Column;
import org.springframework.data.cassandra.core.entitystore.ColumnFamily;

/**
 * The Jobs Column Family for persisting to Cassandra
 * 
 * @author David Webb (dwebb@brightmove.com)
 *
 */
@ColumnFamily
@Data
public class Jobs {

	@Id
	private String key;
	
	@Column(name="job_title")
	private String jobTitle;
	
	@Column(name="pay_rate")
	private String payRate;

}
