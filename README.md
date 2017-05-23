[![Spring Data for Apache Cassandra](https://spring.io/badges/spring-data-cassandra/ga.svg)](http://projects.spring.io/spring-data-cassandra/#quick-start)
[![Spring Data for Apache Cassandra](https://spring.io/badges/spring-data-cassandra/snapshot.svg)](http://projects.spring.io/spring-data-cassandra/#quick-start)

# Spring Data for Apache Cassandra

The primary goal of the [Spring Data](http://projects.spring.io/spring-data) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.

The Spring Data for Apache Cassandra project aims to provide a familiar and consistent Spring-based programming model for new datastores while retaining store-specific features and capabilities. The Spring Data for Apache Cassandra project provides integration with the Apache Cassandra database. Key functional areas of Spring Data for Apache Cassandra are a CQL abstraction, a POJO centric model for interacting with an Apache Cassandra tables and easily writing a repository style data access layer.


## Getting Help

For a comprehensive treatment of all the Spring Data for Apache Cassandra features, please refer to:

* the [User Guide](http://docs.spring.io/spring-data/cassandra/docs/current/reference/html/)
* the [JavaDocs](http://docs.spring.io/spring-data/cassandra/docs/current/api/) have extensive comments in them as well.
* the home page of [Spring Data for Apache Cassandra](http://projects.spring.io/spring-data-cassandra) contains links to articles and other resources.
* for more detailed questions, use [Spring Data for Apache Cassandra on Stackoverflow](http://stackoverflow.com/questions/tagged/spring-data-cassandra).

If you are new to Spring as well as to Spring Data, look for information about [Spring projects](http://projects.spring.io/).


## Quick Start

Prerequisites:
* Java 6
* [DataStax Java Driver for Apache Cassandra 3.x](https://docs.datastax.com/en/developer/driver-matrix/doc/javaDrivers.html)
* Apache Cassandra 1.x, 2.x or 3.x

### Maven configuration

Add the Maven dependency:

```xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-cassandra</artifactId>
  <version>${version}.RELEASE</version>
</dependency>
```

If you would rather like the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

```xml
<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-cassandra</artifactId>
  <version>${version}.BUILD-SNAPSHOT</version>
</dependency>

<repository>
  <id>spring-libs-snapshot</id>
  <name>Spring Snapshot Repository</name>
  <url>http://repo.spring.io/libs-snapshot</url>
</repository>
```

### CassandraTemplate

`CassandraTemplate` is the central support class for Cassandra database operations. It provides:

* Increased productivity by handling common Cassandra operations properly. Includes integrated object mapping between CQL Tables and POJOs.
* Exception translation into Spring's [technology agnostic DAO exception hierarchy](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/dao.html#dao-exceptions).
* Feature rich object mapping integrated with Spring’s Conversion Service.
* Annotation-based mapping metadata but extensible to support other metadata formats.


### Spring Data repositories

To simplify the creation of data repositories Spring Data for Apache Cassandra provides a generic repository programming model. It will automatically create a repository proxy for you that adds implementations of finder methods you specify on an interface.  

For example, given a `Person` class with first and last name properties, a `PersonRepository` interface that can query for `Person` by last name and when the first name matches a like expression is shown below:

```java
public interface PersonRepository extends CrudRepository<Person, Long> {

  List<Person> findByLastname(String lastname);

  List<Person> findByFirstnameLike(String firstname);
}
```

The queries issued on execution will be derived from the method name. Extending `CrudRepository` causes CRUD methods being pulled into the interface so that you can easily save and find single entities and collections of them.

You can have Spring automatically create a proxy for the interface by using the following JavaConfig:

```java
@Configuration
@EnableCassandraRepositories
class ApplicationConfig extends AbstractCassandraConfiguration {

  @Override
  public String getContactPoints() {
    return "localhost";
  }

  @Override
  protected String getKeyspaceName() {
    return "springdata";
  }
}
```

This sets up a connection to a local Apache Cassandra instance and enables the detection of Spring Data repositories (through `@EnableCassandraRepositories`). The same configuration would look like this in XML:

```xml
<cassandra:cluster contact-points="localhost" port="9042" />

<cassandra:session keyspace-name="springdata" />

<cassandra:template id="cassandraTemplate" />

<cassandra:repositories base-package="com.acme.repository" />
```

This will find the repository interface and register a proxy object in the container. You can use it as shown below:

```java
@Service
public class MyService {

  private final PersonRepository repository;

  @Autowired
  public MyService(PersonRepository repository) {
    this.repository = repository;
  }

  public void doWork() {

     repository.deleteAll();

     Person person = new Person();
     person.setFirstname("Oliver");
     person.setLastname("Gierke");
     person = repository.save(person);

     List<Person> lastNameResults = repository.findByLastname("Gierke");
     List<Person> firstNameResults = repository.findByFirstnameLike("Oli*");
 }
}
```

## What's included

Spring Data for Apache Cassandra consists of two modules:
  
* Spring CQL
* Spring Data for Apache Cassandra

You can choose among several approaches to form the basis for your Cassandra database access. Spring’s support for Apache Cassandra comes in different flavors. Once you start using one of these approaches, you can still mix and match to include a feature from a different approach.

### Spring CQL

Spring CQL takes care of all the low-level details that can make Cassandra and CQL such a tedious API to develop with.

`CqlTemplate` is the classic Spring CQL approach and the most popular. This "lowest level" approach and all others use a `CqlTemplate` under the covers including schema generation support.

### Spring Data Cassandra

Spring Data for Apache Cassandra adds object mapping, schema generation and repository support to the feature set.

`CassandraTemplate` wraps a `CqlTemplate` to provide result to object mapping and the use of `SELECT`, `INSERT`, `UPDATE` and `DELETE` methods instead of writing CQL statements. This approach provides better documentation and ease of use. Schema generation support supports fast bootstrapping by using mapped objects to create tables and user types.

## Contributing to Spring Data

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on Stackoverflow and help out on the [spring-data-cassandra](http://stackoverflow.com/questions/tagged/spring-data-cassandra) tag by responding to questions and joining the debate.
* Create [JIRA](https://jira.spring.io/browse/DATACASS) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://spring.io/blog) to spring.io.

Before we accept a non-trivial patch or pull request we will need you to [sign the Contributor License Agreement](https://cla.pivotal.io/sign/spring). Signing the contributor’s agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do. If you forget to do so, you'll be reminded when you submit a pull request. Active contributors might be asked to join the core team, and given the ability to merge pull requests.

## Initial Contributors

Spring Data for Apache Cassandra was initially created and supported by the following
companies and individuals:

* David Webb
* Matthew Adams
* John McPeek
* [Prowave Consulting](http://www.prowaveconsulting.com) - David Webb
* [SciSpike](http://www.scispike.com) - Matthew Adams
