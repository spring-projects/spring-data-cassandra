# Spring Data Cassandra Project Info

## Quick Start

To begin working with ``spring-cassandra`` or ``spring-data-cassandra``, add the Spring Maven Snapshot Repository to your ``pom.xml``.

	<repository>
		<id>spring-libs-snapshot</id>
  		<url>http://repo.spring.io/libs-snapshot</url>
		<snapshots>
			<enabled>true</enabled>
		</snapshots>
	</repository>

### CQL Only (spring-cql)

*Maven Coordinates*

	<dependency>
		<groupId>org.springframework.data</groupId>
		<artifactId>spring-cql</artifactId>
		<version>1.0.0.BUILD-SNAPSHOT</version>
	</dependency>

*Minimal Spring XML Configuration*

	<cql:cluster />
	<cql:session keyspace-name="sensors" />
	<cql:template />

*Minimal Spring JavaConfig*

	@Configuration
	public class MyConfig extends AbstractSessionConfiguration {
		
		@Override
		public String getKeyspaceName() {
			return "sensors";
		}
		
		@Bean
		public CqlOperations cqlTemplate() {
			return new CqlTemplate(session.getObject());
		}
	}

*Application Class*

	public class SensorService {
		
		@Autowired
		CqlOperations template;
		
		// ...
	}
	
### CQL and Object Mapping (spring-data-cassandra)

*Maven Coordinates*

	<dependency>
		<groupId>org.springframework.data</groupId>
		<artifactId>spring-data-cassandra</artifactId>
		<version>1.0.0.BUILD-SNAPSHOT</version>
	</dependency>

*Minimal Spring XML Configuration*

	<cassandra:cluster />
	<cassandra:session keyspace-name="foobar" />
	<cassandra:repositories base-package="org.example.domain" />

*Minimal Spring JavaConfig*

	@Configuration
	@EnableCassandraRepositories(basePackage = "org.example.domain")
	public class MyConfig extends AbstractSpringDataCassandraConfiguration {
		
		@Override
		public String getKeyspaceName() {
			return "foobar";
		}
	}

*Application Class*

	public class SensorService {
		
		@Autowired
		SensorRepository repo;
		
		// ...
	}
	

## Release Preview

The goal of this release preview is to publish the pieces of spring-data-cassandra as they become available
so that users of the module can start to familiarize themselves with the components, and ultimately to provide
the development team feedback.  We hope this iterative approach produces the most usable and developer friendly
``spring-data-cassandra`` repository.

### What's included (Q1 - 2014)

There are two modules included in the ``spring-data-cassandra`` repository:  ``spring-cassandra`` and ``spring-data-cassandra``.

_Note: we are considering consolidating both modules into one for convenience._

#### Module ``spring-cassandra``

This is the low-level core template framework, like the ones you are used to using on all your Spring projects.  Our
``CqlTemplate`` provides everything you need for working with Cassandra using Spring's familiar template pattern in a manner very similar to Spring's ``JdbcTemplate``.

This includes persistence exception translation, Spring JavaConfig and XML configuration support.  Define your Spring beans to setup your
Cassandra ``Cluster`` object, then create your ``Session`` and you are ready to interact with Cassandra using the ``CqlTemplate``.

The module also offers convenient builder pattern classes to easily specify the creation, alteration, and dropping of keyspaces via a fluent API.  They are intended to be used with generators that produce CQL that can then be easily executed by ``CqlTemplate``.  See test class ``CreateTableCqlGeneratorTests`` for examples.  Don't forget to check out class ``MapBuilder`` for easy creation of Cassandra ``TableOption`` values.  The builders & CQL generators for ``CREATE TABLE``, ``ALTER TABLE``, and ``DROP TABLE`` operations are

* ``CreateTableSpecification`` & ``CreateTableCqlGenerator``,
* ``AlterTableSpecification`` & ``AlterTableCqlGenerator``, and
* ``DropTableSpecification`` & ``DropTableCqlGenerator``, respectively.

The support for Spring JavaConfig and a Spring Cassandra XML namespace makes it easy to configure your context to work with Cassandra, including XML namespace support for automatic keyspace creations, drops & more, which can be convenient when writing integration tests.

#### Module ``spring-data-cassandra``

The ``spring-data-cassandra`` module depends on the ``spring-cassandra`` module and adds the familiar Spring Data features like repositories and lightweight POJO persistence.

__Note: As of 7 Feb 2014, module ``spring-data-cassandra`` is functional!__

We are actively working on its completion, but wanted to make the lower level Cassandra template functionality available to the Spring and Cassandra communities.

#### Best practices

We have worked closely with the DataStax Driver Engineering team to ensure that our implementation around their native
CQL Driver takes advantage of all that it has to offer. If you need access to more than one keyspace in your application,
create more than one ``CqlTemplate`` (one per session, one session per keyspace), then use the appropriate template instance where needed.

Here are some considerations when designing your application for use with ``spring-cassandra``.

* When creating a template, wire in a single ``Session`` per keyspace.
* ``Session`` is threadsafe, so only use one per keyspace per application context!
* __Do not issue__ ``USE <keyspace>`` __commands__ on your session; instead, _configure_ the keyspace name you intend to use.
* The DataStax Java Driver handles all failover and retry logic for you.  Become familiar with the [Driver Documentation](http://www.datastax.com/documentation/developer/java-driver/1.0/webhelp/index.html), which will help you configure your ``Cluster``.
* If you are using a Cassandra ``Cluster`` spanning multiple data centers, please be insure to include hosts from all data centers in your contact points.

#### High Performance Ingestion

We have included a variety of overloaded ``ingest()`` methods in ``CqlTemplate`` for high performance batch writes.

### What's Next (early Q1 - 2014):  Spring _Data_ Cassandra

The next round of work to do is to complete module ``spring-data-cassandra``, while taking feedback from the community's use of module ``spring-cassandra``.  We are already well on our way to completion.

#### Cassandra Repository

The base Spring Data Repository interface for Cassandra is ``CassandraRepository``.

#### Cassandra Template

``CassandraTemplate`` extends ``CqlTemplate`` to provide even more interaction with Cassandra using annotated POJOs.
The Spring Data ``CassandraRepository`` implementation is a client of ``CassandraTemplate``.  This _data_ template gives the developer the capability of working with annotated POJOs and the template pattern without the requirement of the Spring Data ``Repository`` interface, in addition to the familiar Spring Data ``Repository`` concepts.

#### Cassandra Admin Template

This is another Spring template class to help you with all of your keyspace and table administration tasks.

#### Official Reference Guide

Once we have all the inner workings of the ``CassandraRepository`` interface completed, we will publish a full reference guide on using all of the features in ``spring-data-cassandra``.

## CqlTemplate Examples

### JavaConfig

Here is a very basic example to get your project connected to Cassandra running on your local machine.

	@Configuration
	public class TestConfig extends AbstractCassandraConfiguration {

		public static final String KEYSPACE = "test";

		@Override
		protected String getKeyspaceName() {
			return KEYSPACE;
		}

		@Bean
		public CqlOperations CqlTemplate() {
			return new CqlTemplate(session().getObject());
		}
	}

### XML Configuration

	<cassandra-cluster />
	<cassandra-session keyspace-name="test" />
	<cassandra-template />

### Using CqlTemplate

	public class CqlOperationsTest {

		@Autowired
		private CqlOperations template;
		
		public Integer getCount() throws DataAccessException {
		
			String cql = "select count(*) from table_name where id='12345'";
			
			Integer count = template.queryForObject(cql, Integer.class);
			
			log.info("Row Count is -> " + count);
			
			return count;
		}
	}

## Current Status

This community-led [Spring Data](http://projects.spring.io/spring-data)
subproject is well underway.

A first milestone (M1) is expected sometime in early 1Q14 built on the
following artifacts (or more recent versions thereof):

* Spring Data Commons 1.8.x
* Cassandra 2.0
* Datastax Java Driver 2.0.X
* JDK 1.6+

The GA release is expected as part of the Spring
Data release train [Dijkstra](https://github.com/spring-projects/spring-data-commons/wiki/Release-Train-Dijkstra).


## Source Repository & Issue Tracking

Source for this module is hosted on GitHub in [Spring Projects](https://github.com/spring-projects/spring-data-cassandra).  
The Spring Data Cassandra JIRA can be found [here](https://jira.springsource.org/browse/DATACASS).

## Reporting Problems

If you encounter a problem using this module, please report the issue to us using [JIRA](https://jira.springsource.org/browse/DATACASS).  Please provide as much information as you can, including:

* Cassandra version
* Community or DataStax distribution of Cassandra
* JDK Version
* Output of ``show schema`` where applicable
* Any stacktraces showing the location of the problem
* Steps to reproduce
* Any other relevant information that you would like to see if you were diagnosing the problem.

Please do not post anything to Jira or the mailing list on how to use Cassandra.  That is beyond the scope
of this module and there are a variety of resources available targeting that specific subject.

We recommend reading the following:

* [PlanetCassandra.org](http://planetcassandra.org/)
* [Getting Started](http://wiki.apache.org/cassandra/GettingStarted)
* [Driver Documentation](http://www.datastax.com/documentation/developer/java-driver/1.0/webhelp/index.html)


## Contact

For more information, feel free to contact the individuals listed
below:

* David Webb:  dwebb _at_ prowaveconsulting _dot_ com
* Matthew Adams:  matthew _dot_ adams _at_ scispike _dot_ com

Also, developer discussions are being hosted via a [Spring Data Cassandra Google Group](https://groups.google.com/forum/#!forum/spring-data-cassandra).

## Contributing Individuals

* David Webb
* Matthew Adams
* John McPeek

## Sponsoring Companies

Spring Data Cassandra is being led and supported by the following
companies and individuals:

* [Prowave Consulting](http://www.prowaveconsulting.com) - David Webb
* [SciSpike](http://www.scispike.com) - Matthew Adams
* [VHA](http://www.vha.com)

The following companies and individuals are also generously providing
support:

* [DataStax](http://www.datastax.com)
* [Spring](http://www.spring.io) @ [Pivotal](http://www.gopivotal.com)
