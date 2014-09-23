package org.springframework.cassandra.test.integration.core.template.async;

import org.springframework.cassandra.core.AsynchronousQueryListener;
import org.springframework.cassandra.test.unit.support.TestListener;

import com.datastax.driver.core.ResultSetFuture;

class BasicListener extends TestListener implements AsynchronousQueryListener {

	ResultSetFuture rsf;

	@Override
	public void onQueryComplete(ResultSetFuture rsf) {
		countDown();
		this.rsf = rsf;
	}
}
