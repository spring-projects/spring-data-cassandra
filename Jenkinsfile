def p = [:]
node {
    checkout scm
    p = readProperties interpolate: true, file: 'ci/pipeline.properties'
}

pipeline {
	agent none

	triggers {
		pollSCM 'H/10 * * * *'
		upstream(upstreamProjects: "spring-data-commons/2.5.x", threshold: hudson.model.Result.SUCCESS)
	}

	options {
		disableConcurrentBuilds()
		buildDiscarder(logRotator(numToKeepStr: '14'))
	}

	stages {
		stage("Docker images") {
			parallel {
				stage('Publish JDK (main) + Cassandra 3.11') {
					when {
					    anyOf {
						    changeset "ci/openjdk8-cassandra-3.11/**"
						    changeset "ci/pipeline.properties"
						}
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-with-cassandra-3.11:${p['java.main.tag']}", "--build-arg BASE=${p['docker.java.main.image']} --build-arg CASSANDRA=${p['docker.cassandra.3.version']} ci/openjdk8-cassandra-3.11/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK (next) + Cassandra 3.11') {
					when {
					    anyOf {
						    changeset "ci/openjdk11-8-cassandra-3.11/**"
						    changeset "ci/pipeline.properties"
                        }
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-with-cassandra-3.11:${p['java.11.tag']}", "--build-arg BASE=${p['docker.java.11.image']} --build-arg CASSANDRA=${p['docker.cassandra.3.version']} ci/openjdk11-8-cassandra-3.11/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 15 + Cassandra 3.11') {
					when {
					    anyOf {
						    changeset "ci/openjdk15-8-cassandra-3.11/**"
						    changeset "ci/pipeline.properties"
						}
					}
					agent { label 'data' }
					options { timeout(time: 30, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-with-cassandra-3.11:${p['java.15.tag']}", "--build-arg BASE=${p['docker.java.15.image']} --build-arg CASSANDRA=${p['docker.cassandra.3.version']} ci/openjdk17-8-cassandra-3.11/")
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								image.push()
							}
						}
					}
				}
			}
		}

		stage("test: baseline (main)") {
			when {
				anyOf {
					branch '3.2.x'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				label 'data'
			}
			options { timeout(time: 30, unit: 'MINUTES') }
			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}
			steps {
				script {
					docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
						docker.image("springci/spring-data-with-cassandra-3.11:${p['java.main.tag']}").inside(p['docker.java.inside.basic']) {
							sh 'mkdir -p /tmp/jenkins-home'
							sh 'JAVA_HOME=/opt/java/openjdk /opt/cassandra/bin/cassandra -R &'
							sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Pci,external-cassandra clean dependency:list verify -Dsort -U -B'
						}
					}
				}
			}
		}

		stage("Test other configurations") {
			when {
				allOf {
					branch '3.2.x'
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
						ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
					}
					steps {
						script {
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								docker.image("springci/spring-data-with-cassandra-3.11:${p['java.11.tag']}").inside(p['docker.java.inside.basic']) {
									sh 'mkdir -p /tmp/jenkins-home'
									sh 'JAVA_HOME=/opt/java/openjdk8 /opt/cassandra/bin/cassandra -R &'
									sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Pci,external-cassandra,java11 clean dependency:list verify -Dsort -U -B'
								}
							}
						}
					}
				}
				stage("test: baseline (jdk15)") {
					agent {
						label 'data'
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					environment {
						ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
					}
					steps {
						script {
							docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
								docker.image("springci/spring-data-with-cassandra-3.11:${p['java.15.tag']}").inside(p['docker.java.inside.basic']) {
									sh 'mkdir -p /tmp/jenkins-home'
									sh 'JAVA_HOME=/opt/java/openjdk8 /opt/cassandra/bin/cassandra -R &'
									sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Pci,external-cassandra,java11 clean dependency:list verify -Dsort -U -B'
								}
							}
						}
					}
				}
			}
		}
		stage('Release to artifactory') {
			when {
				anyOf {
					branch '3.2.x'
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				label 'data'
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				script {
					docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
						docker.image(p['docker.java.main.image']).inside(p['docker.java.inside.basic']) {
							sh 'mkdir -p /tmp/jenkins-home'
							sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Pci,artifactory ' +
									'-Dartifactory.server=https://repo.spring.io ' +
									"-Dartifactory.username=${ARTIFACTORY_USR} " +
									"-Dartifactory.password=${ARTIFACTORY_PSW} " +
									"-Dartifactory.staging-repository=libs-snapshot-local " +
									"-Dartifactory.build-name=spring-data-cassandra " +
									"-Dartifactory.build-number=${BUILD_NUMBER} " +
									'-Dmaven.test.skip=true clean deploy -U -B'
						}
					}
				}
			}
		}
		stage('Publish documentation') {
			when {
				branch '3.2.x'
			}
			agent {
				label 'data'
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
			}

			steps {
				script {
					docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
						docker.image('adoptopenjdk/openjdk8:latest').inside('-v $HOME:/tmp/jenkins-home') {
							sh 'mkdir -p /tmp/jenkins-home'
							sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Pci,distribute ' +
									'-Dartifactory.server=https://repo.spring.io ' +
									"-Dartifactory.username=${ARTIFACTORY_USR} " +
									"-Dartifactory.password=${ARTIFACTORY_PSW} " +
									"-Dartifactory.distribution-repository=temp-private-local " +
									'-Dmaven.test.skip=true clean deploy -U -B'
						}
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
