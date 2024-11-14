def p = [:]
node {
	checkout scm
	p = readProperties interpolate: true, file: 'ci/pipeline.properties'
}

pipeline {
	agent none

	triggers {
		pollSCM 'H/10 * * * *'
		upstream(upstreamProjects: "spring-data-commons/main", threshold: hudson.model.Result.SUCCESS)
	}

	options {
		disableConcurrentBuilds()
		buildDiscarder(logRotator(numToKeepStr: '14'))
	}

	stages {
		stage("Docker images") {
			parallel {
				stage('Publish JDK 17 + Cassandra 3.11') {
					when {
						anyOf {
							changeset "ci/openjdk17-8-cassandra-3.11/**"
							changeset "ci/pipeline.properties"
						}
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						script {
							def image = docker.build("springci/spring-data-with-cassandra-3.11:${p['java.main.tag']}", "--build-arg BASE=${p['docker.java.main.image']} --build-arg CASSANDRA=${p['docker.cassandra.3.version']} ci/openjdk17-8-cassandra-3.11/")
							docker.withRegistry(p['docker.registry'], p['docker.credentials']) {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK.next + Cassandra 3.11') {
					when {
						anyOf {
							changeset "ci/openjdk23-8-cassandra-3.11/**"
							changeset "ci/pipeline.properties"
						}
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }
					steps {
						script {
							def image = docker.build("springci/spring-data-with-cassandra-3.11:${p['java.next.tag']}", "--build-arg BASE=${p['docker.java.next.image']} --build-arg CASSANDRA=${p['docker.cassandra.3.version']} ci/openjdk23-8-cassandra-3.11/")
							docker.withRegistry(p['docker.registry'], p['docker.credentials']) {
								image.push()
							}
						}
					}
				}
			}
		}

		stage("test: baseline (main)") {
			when {
				beforeAgent(true)
				anyOf {
					branch(pattern: "main|(\\d\\.\\d\\.x)", comparator: "REGEXP")
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				label 'data'
			}
			options { timeout(time: 30, unit: 'MINUTES') }
			environment {
				ARTIFACTORY = credentials("${p['artifactory.credentials']}")
				DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
			}
			steps {
				script {
					docker.withRegistry(p['docker.proxy.registry'], p['docker.proxy.credentials']) {
						docker.image("springci/spring-data-with-cassandra-3.11:${p['java.main.tag']}").inside(p['docker.java.inside.docker']) {
							sh 'mkdir -p /tmp/jenkins-home'
							sh 'JAVA_HOME=/opt/java/openjdk8 /opt/cassandra/bin/cassandra -R &'
							sh 'MAVEN_OPTS="-Duser.name=' + "${p['jenkins.user.name']}" + ' -Duser.home=/tmp/jenkins-home" ' +
								"./mvnw -s settings.xml -Pci,external-cassandra -Ddevelocity.storage.directory=/tmp/jenkins-home/.develocity-root -Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-cassandra " +
								"clean dependency:list verify -Dsort -U -B"
						}
					}
				}
			}
		}

		stage("Test other configurations") {
			when {
				beforeAgent(true)
				allOf {
					branch(pattern: "main|(\\d\\.\\d\\.x)", comparator: "REGEXP")
					not { triggeredBy 'UpstreamCause' }
				}
			}
			parallel {
				stage("test: baseline (next)") {
					agent {
						label 'data'
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					environment {
						ARTIFACTORY = credentials("${p['artifactory.credentials']}")
						DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
					}
					steps {
						script {
							docker.withRegistry(p['docker.proxy.registry'], p['docker.proxy.credentials']) {
								docker.image("springci/spring-data-with-cassandra-3.11:${p['java.next.tag']}").inside(p['docker.java.inside.docker']) {
									sh 'mkdir -p /tmp/jenkins-home'
									sh 'JAVA_HOME=/opt/java/openjdk8 /opt/cassandra/bin/cassandra -R &'
									sh 'MAVEN_OPTS="-Duser.name=' + "${p['jenkins.user.name']}" + ' -Duser.home=/tmp/jenkins-home" ' +
										"./mvnw -s settings.xml -Pci,external-cassandra -Ddevelocity.storage.directory=/tmp/jenkins-home/.develocity-root -Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-cassandra " +
										"clean dependency:list verify -Dsort -U -B"
								}
							}
						}
					}
				}
			}
		}

		stage('Release to artifactory') {
			when {
				beforeAgent(true)
				anyOf {
					branch(pattern: "main|(\\d\\.\\d\\.x)", comparator: "REGEXP")
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				label 'data'
			}
			options { timeout(time: 20, unit: 'MINUTES') }
			environment {
				ARTIFACTORY = credentials("${p['artifactory.credentials']}")
				DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
			}
			steps {
				script {
					docker.withRegistry(p['docker.proxy.registry'], p['docker.proxy.credentials']) {
						docker.image(p['docker.java.main.image']).inside(p['docker.java.inside.docker']) {
							sh 'mkdir -p /tmp/jenkins-home'
							sh 'MAVEN_OPTS="-Duser.name=' + "${p['jenkins.user.name']}" + ' -Duser.home=/tmp/jenkins-home" ' +
									"./mvnw -s settings.xml -Pci,artifactory " +
									"-Ddevelocity.storage.directory=/tmp/jenkins-home/.develocity-root " +
									"-Dartifactory.server=${p['artifactory.url']} " +
									"-Dartifactory.username=${ARTIFACTORY_USR} " +
									"-Dartifactory.password=${ARTIFACTORY_PSW} " +
									"-Dartifactory.staging-repository=${p['artifactory.repository.snapshot']} " +
									"-Dartifactory.build-name=spring-data-cassandra " +
									"-Dartifactory.build-number=spring-data-cassandra-${BRANCH_NAME}-build-${BUILD_NUMBER} " +
									"-Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-cassandra " +
									"-Dmaven.test.skip=true clean deploy -U -B"
						}
					}
				}
			}
		}
	}

	post {
		changed {
			script {
				emailext(
						subject: "[${currentBuild.fullDisplayName}] ${currentBuild.currentResult}",
						mimeType: 'text/html',
						recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']],
						body: "<a href=\"${env.BUILD_URL}\">${currentBuild.fullDisplayName} is reported as ${currentBuild.currentResult}</a>")
			}
		}
	}
}
