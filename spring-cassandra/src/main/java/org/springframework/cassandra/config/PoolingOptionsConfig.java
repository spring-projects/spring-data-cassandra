/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.config;

/**
 * Pooling options POJO. Can be remote or local.
 * 
 * @author Alex Shvid
 */
public class PoolingOptionsConfig {

	private Integer minSimultaneousRequests;
	private Integer maxSimultaneousRequests;
	private Integer coreConnections;
	private Integer maxConnections;

	public Integer getMinSimultaneousRequests() {
		return minSimultaneousRequests;
	}

	public void setMinSimultaneousRequests(Integer minSimultaneousRequests) {
		this.minSimultaneousRequests = minSimultaneousRequests;
	}

	public Integer getMaxSimultaneousRequests() {
		return maxSimultaneousRequests;
	}

	public void setMaxSimultaneousRequests(Integer maxSimultaneousRequests) {
		this.maxSimultaneousRequests = maxSimultaneousRequests;
	}

	public Integer getCoreConnections() {
		return coreConnections;
	}

	public void setCoreConnections(Integer coreConnections) {
		this.coreConnections = coreConnections;
	}

	public Integer getMaxConnections() {
		return maxConnections;
	}

	public void setMaxConnections(Integer maxConnections) {
		this.maxConnections = maxConnections;
	}

}
