/*
 * Copyright 2014-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.cdi;

import jakarta.inject.Inject;

/**
 * @author Mohsin Husen
 * @author Oliver Gierke
 * @author Mark Paluch
 */
class CdiRepositoryClient {

	private CdiUserRepository repository;
	private QualifiedUserRepository qualifiedUserRepository;
	private SamplePersonRepository samplePersonRepository;

	public CdiUserRepository getRepository() {
		return repository;
	}

	@Inject
	public void setRepository(CdiUserRepository repository) {
		this.repository = repository;
	}

	public SamplePersonRepository getSamplePersonRepository() {
		return samplePersonRepository;
	}

	@Inject
	public void setSamplePersonRepository(SamplePersonRepository samplePersonRepository) {
		this.samplePersonRepository = samplePersonRepository;
	}

	public QualifiedUserRepository getQualifiedUserRepository() {
		return qualifiedUserRepository;
	}

	@Inject
	public void setQualifiedUserRepository(@UserDB @OtherQualifier QualifiedUserRepository qualifiedUserRepository) {
		this.qualifiedUserRepository = qualifiedUserRepository;
	}
}
