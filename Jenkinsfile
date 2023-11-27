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
				stage('Publish JDK 21 + Cassandra 3.11') {
					when {
						anyOf {
							changeset "ci/openjdk21-8-cassandra-3.11/**"
							changeset "ci/pipeline.properties"
						}
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-with-cassandra-3.11:${p['java.next.tag']}", "--build-arg BASE=${p['docker.java.next.image']} --build-arg CASSANDRA=${p['docker.cassandra.3.version']} ci/openjdk21-8-cassandra-3.11/")
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
				DEVELOCITY_CACHE = credentials("${p['develocity.cache.credentials']}")
				DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
			}
			steps {
				script {
					docker.image("harbor-repo.vmware.com/dockerhub-proxy-cache/springci/spring-data-with-cassandra-3.11:${p['java.main.tag']}").inside('-v $HOME:/tmp/jenkins-home') {
						sh 'mkdir -p /tmp/jenkins-home'
						sh 'JAVA_HOME=/opt/java/openjdk8 /opt/cassandra/bin/cassandra -R &'
						sh 'MAVEN_OPTS="-Duser.name=' + "${p['jenkins.user.name']}" + ' -Duser.home=/tmp/jenkins-home" ' +
							"DEVELOCITY_CACHE_USERNAME=${DEVELOCITY_CACHE_USR} " +
							"DEVELOCITY_CACHE_PASSWORD=${DEVELOCITY_CACHE_PSW} " +
							"GRADLE_ENTERPRISE_ACCESS_KEY=${DEVELOCITY_ACCESS_KEY} " +
							"./mvnw -s settings.xml -Pci,external-cassandra " +
							"clean dependency:list verify -Dsort -U -B"
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
						DEVELOCITY_CACHE = credentials("${p['develocity.cache.credentials']}")
						DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
					}
					steps {
						script {
							docker.image("harbor-repo.vmware.com/dockerhub-proxy-cache/springci/spring-data-with-cassandra-3.11:${p['java.next.tag']}").inside('-v $HOME:/tmp/jenkins-home') {
								sh 'mkdir -p /tmp/jenkins-home'
								sh 'JAVA_HOME=/opt/java/openjdk8 /opt/cassandra/bin/cassandra -R &'
								sh 'MAVEN_OPTS="-Duser.name=' + "${p['jenkins.user.name']}" + ' -Duser.home=/tmp/jenkins-home" ' +
									"DEVELOCITY_CACHE_USERNAME=${DEVELOCITY_CACHE_USR} " +
									"DEVELOCITY_CACHE_PASSWORD=${DEVELOCITY_CACHE_PSW} " +
									"GRADLE_ENTERPRISE_ACCESS_KEY=${DEVELOCITY_ACCESS_KEY} " +
									"./mvnw -s settings.xml -Pci,external-cassandra " +
									"clean dependency:list verify -Dsort -U -B"
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
				DEVELOCITY_CACHE = credentials("${p['develocity.cache.credentials']}")
				DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
			}

			steps {
				script {
					docker.image(p['docker.java.main.image']).inside(p['docker.java.inside.basic']) {
						sh 'mkdir -p /tmp/jenkins-home'
						sh 'MAVEN_OPTS="-Duser.name=' + "${p['jenkins.user.name']}" + ' -Duser.home=/tmp/jenkins-home" ' +
								"DEVELOCITY_CACHE_USERNAME=${DEVELOCITY_CACHE_USR} " +
								"DEVELOCITY_CACHE_PASSWORD=${DEVELOCITY_CACHE_PSW} " +
								"GRADLE_ENTERPRISE_ACCESS_KEY=${DEVELOCITY_ACCESS_KEY} " +
								"./mvnw -s settings.xml -Pci,artifactory " +
								"-Dartifactory.server=${p['artifactory.url']} " +
								"-Dartifactory.username=${ARTIFACTORY_USR} " +
								"-Dartifactory.password=${ARTIFACTORY_PSW} " +
								"-Dartifactory.staging-repository=${p['artifactory.repository.snapshot']} " +
								"-Dartifactory.build-name=spring-data-cassandra " +
								"-Dartifactory.build-number=${BUILD_NUMBER} " +
								"-Dmaven.test.skip=true clean deploy -U -B"
					}
				}
			}
		}
	}

	post {
		changed {
			script {
				slackSend(
						color: (currentBuild.currentResult == 'SUCCESS') ? 'good' : 'danger',
						channel: '#spring-data-dev',
						message: "${currentBuild.fullDisplayName} - `${currentBuild.currentResult}`\n${env.BUILD_URL}")
				emailext(
						subject: "[${currentBuild.fullDisplayName}] ${currentBuild.currentResult}",
						mimeType: 'text/html',
						recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']],
						body: "<a href=\"${env.BUILD_URL}\">${currentBuild.fullDisplayName} is reported as ${currentBuild.currentResult}</a>")
			}
		}
	}
}
