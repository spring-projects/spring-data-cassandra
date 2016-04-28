# Spring Data Cassandra Project Info

## Quick Start

To begin working with ``spring-cql`` or ``spring-data-cassandra``, add the Spring Maven Snapshot Repository to your ``pom.xml``.

### Releases

Generally available releases are available in Maven Central.

### Snapshots

	<repository>
		<id>spring-libs-snapshot</id>
  		<url>http://repo.spring.io/libs-snapshot</url>
		<snapshots>
			<enabled>true</enabled>
		</snapshots>
	</repository>

### Raw CQL only with no POJO mapping (``spring-cql``)

*Maven Coordinates*

	<dependency>
		<groupId>org.springframework.data</groupId>
		<artifactId>spring-cql</artifactId>
		<version>1.0.0.RELEASE (or whatever your preferred version is)</version>
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
	
### CQL and POJO mapping (``spring-data-cassandra``)

*Maven Coordinates*

	<dependency>
		<groupId>org.springframework.data</groupId>
		<artifactId>spring-data-cassandra</artifactId>
		<version>1.0.0.RELEASE (or whatever your preferred version is)</version>
	</dependency>

*Minimal Spring XML Configuration*

	<cassandra:cluster />
	<cassandra:session keyspace-name="foobar" />
	<cassandra:repositories base-package="org.example.domain" />

*Minimal Spring JavaConfig*

	@Configuration
	@EnableCassandraRepositories(basePackages = "org.example.domain")
	public class MyConfig extends AbstractCassandraConfiguration {
		
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
	

## What's included

There are two modules included in the ``spring-data-cassandra`` repository:  ``spring-cql`` and ``spring-data-cassandra``.

### Module ``spring-cql``:  Pure CQL support a la Spring's template pattern

This is the low-level core template framework, like the ones you are used to using on all your Spring projects.  Our
``CqlTemplate`` provides everything you need for working with Cassandra using Spring's familiar template pattern in a manner very similar to Spring's ``JdbcTemplate``.

This includes persistence exception translation, Spring JavaConfig and XML configuration support.  Define your Spring beans to setup your
Cassandra ``Cluster`` object, then create your ``Session`` and you are ready to interact with Cassandra using the ``CqlTemplate``.

The module also offers convenient builder pattern classes to easily specify the creation, alteration, and dropping of keyspaces via a fluent API.  They are intended to be used with generators that produce CQL that can then be easily executed by ``CqlTemplate``.  See test class ``CreateTableCqlGeneratorTests`` for examples.  Don't forget to check out class ``MapBuilder`` for easy creation of Cassandra ``TableOption`` values.  The builders & CQL generators for ``CREATE TABLE``, ``ALTER TABLE``, and ``DROP TABLE`` operations are

* ``CreateTableSpecification`` & ``CreateTableCqlGenerator``,
* ``AlterTableSpecification`` & ``AlterTableCqlGenerator``, and
* ``DropTableSpecification`` & ``DropTableCqlGenerator``, respectively.

The support for Spring JavaConfig and a Spring Cassandra XML namespace makes it easy to configure your context to work with Cassandra, including XML namespace support for automatic keyspace creations, drops & more, which can be convenient when writing integration tests.

### Module ``spring-data-cassandra``:  Spring Data POJO persistence over Cassandra

The ``spring-data-cassandra`` module depends on the ``spring-cql`` module and adds the familiar Spring Data features like repositories and lightweight POJO persistence.

### Best practices

We have worked closely with the DataStax Driver Engineering team to ensure that our implementation around their native
CQL Driver takes advantage of all that it has to offer. If you need access to more than one keyspace in your application,
create more than one ``CqlTemplate`` (one ``CqlTemplate`` per session, one session per keyspace), then use the appropriate template instance where needed.

Here are some considerations when designing your application for use with ``spring-cql`` (as well as ``spring-data-cassandra``).

* When creating a template, wire in a single ``Session`` per keyspace.
* ``Session`` is threadsafe, so only use one per keyspace per application context!
* __Do not issue__ ``USE <keyspace>`` __commands__ on your session; instead, _configure_ the keyspace name you intend to use.
* The DataStax Java Driver handles all failover and retry logic for you.  Become familiar with the [Driver Documentation](http://www.datastax.com/documentation/developer/java-driver/1.0/webhelp/index.html), which will help you configure your ``Cluster``.
* If you are using a Cassandra ``Cluster`` spanning multiple data centers, please be insure to include hosts from all data centers in your contact points.

### High Performance Ingestion

We have included a variety of overloaded ``ingest(..)`` methods in ``CqlTemplate`` for high performance batch writes.

### ``CassandraRepository``

The base Spring Data Repository interface for Cassandra is ``CassandraRepository``.  This allows you to simply annotate key fields directly in your entity instead of having to define your own primary key classes, although you still can (see the parent interface of ``CassandraRepository``, called ``TypedIdCassandraRepository<T,ID>``).  In fact, ``CassandraRepository`` uses a provided primary key class called ``MapId``, and declares itself to extend ``TypedIdCassandraRepository<T, MapId>``!

### ``CassandraTemplate``

``CassandraTemplate`` extends ``CqlTemplate``, adding mapping information so you can work with Cassandra using annotated POJOs.
The Spring Data ``CassandraRepository`` implementation also happens to be a client of ``CassandraTemplate`` so, if necessary, a developer can work with annotated POJOs and the template pattern without ever using ``CassandraRepository``.

### ``CassandraAdminTemplate``

This is another Spring template class to help you with your keyspace and table administration tasks.

## ``CqlTemplate`` Examples

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
* [Driver Documentation](http://docs.datastax.com/en/developer/java-driver/2.1/java-driver/whatsNew2.html)


## Contact

For more information, feel free to contact the individuals listed
below:

* David Webb:  dwebb _at_ prowaveconsulting _dot_ com
* Matthew Adams:  matthew _dot_ adams _at_ scispike _dot_ com

Also, developer discussions are being hosted on [StackOverflow](https://www.stackoverflow.com) using the tag ``spring-data-cassandra``.

## Contributing Individuals

* David Webb
* Matthew Adams
* John McPeek

## Sponsoring Companies

Spring Data Cassandra is being led and supported by the following
companies and individuals:

* [Prowave Consulting](http://www.prowaveconsulting.com) - David Webb
* [SciSpike](http://www.scispike.com) - Matthew Adams

The following companies and individuals are also generously providing
support:

* [DataStax](http://www.datastax.com)
* [Spring](http://www.spring.io) @ [Pivotal](http://www.gopivotal.com)
* Docbook Editor provided by [Oxygen XML](http://www.oxygenxml.com)

![Oxygen XML Logo](http://www.oxygenxml.com/img/resources/oxygen190x62.png)
