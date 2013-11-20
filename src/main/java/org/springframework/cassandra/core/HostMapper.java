package org.springframework.cassandra.core;

import java.util.Collection;
import java.util.Set;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.exceptions.DriverException;

public interface HostMapper<T> {

	Collection<T> mapHosts(Set<Host> host) throws DriverException;

}
