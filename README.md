[![Build Status](https://travis-ci.org/DDTH/ddth-lucext.svg?branch=master)](https://travis-ci.org/DDTH/ddth-lucext)

# ddth-lucext

DDTH's utilities and extensions for [Apache Lucene](http://lucene.apache.org).

Project home:
[https://github.com/DDTH/ddth-lucext](https://github.com/DDTH/ddth-lucext)


## Installation

Latest release version: `0.1.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency: if only a sub-set of `ddth-lucext` functionality is used, choose the corresponding
dependency artifact(s) to reduce the number of unused jar files.

*ddth-lucext-core*: (not much functionality at the moment)

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-lucext-core</artifactId>
	<version>0.1.0</version>
</dependency>
```

*ddth-lucext-cassandra*: include *ddth-lucext-core* and [Apache's Cassandra](http://cassandra.apache.org) dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-lucext-cassandra</artifactId>
    <version>0.1.0</version>
    <type>pom</type>
</dependency>
```

*ddth-lucext-redis*: include *ddth-lucext-core* and [Redis](https://redis.io) dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-lucext-redis</artifactId>
    <version>0.1.0</version>
    <type>pom</type>
</dependency>
```


## Usage

### `RedisDirectory`

Store Lucene's data in [Redis](https://redis.io).

```java
import com.github.ddth.commons.redis.*;
import com.github.ddth.lucext.directory.redis.*;

// 1. create a JedisConnector instance
JedisConnector jc = new JedisConnector();
jc.setRedisHostsAndPorts("localhost:6379").setRedisPassword("secret").init();

// 2. create RedisDirectory
Directory DIR = new RedisDirectory(jc).init();

// 3. use the directory normally with Lucene
IndexReader ir = DirectoryReader.open(DIR);
IndexSearcher is = new IndexSearcher(ir);
Analyzer analyzer = new StandardAnalyzer();
QueryParser parser = new QueryParser(null, analyzer);
Query q = parser.parse("...");
TopDocs result = is.search(q, 10);
System.out.println("Hits:" + result.totalHits);
for (ScoreDoc sDoc : result.scoreDocs) {
    Document doc = is.doc(sDoc.doc);
    System.out.println(doc);
}
ir.close();

// 4. close the directory when done
DIR.close();
```

(See more about `JedisConnector` [here](https://github.com/DDTH/ddth-commons/blob/master/ddth-commons-core/src/main/java/com/github/ddth/commons/redis/README.md))

### `CassandraDirectory`

Store Lucene's data in [Cassandra](http://cassandra.apache.org).

```java
import com.github.ddth.cql.*;
import com.github.ddth.lucext.directory.cassandra.*;

// 1. create a SessionManager instance
SessionManager sm = new SessionManager();
sm.setDefaultHostsAndPorts("localhost")
    .setDefaultUsername("cassandra-user")
    .setDefaultPassword("cassandra-password)"
    .setDefaultKeyspace(null)
    .init();

// 2. create CassandraDirectory
Directory DIR = new CassandraDirectory(sm).setKeyspace("mykeyspace").init();

// 3. use the directory normally with Lucene
IndexReader ir = DirectoryReader.open(DIR);
IndexSearcher is = new IndexSearcher(ir);
Analyzer analyzer = new StandardAnalyzer();
QueryParser parser = new QueryParser(null, analyzer);
Query q = parser.parse("...");
TopDocs result = is.search(q, 10);
System.out.println("Hits:" + result.totalHits);
for (ScoreDoc sDoc : result.scoreDocs) {
    Document doc = is.doc(sDoc.doc);
    System.out.println(doc);
}
ir.close();

// 4. close the directory when done
DIR.close();
```

(See more about `SessionManager` [here](https://github.com/DDTH/ddth-cql-utils))

`CassandraDirectory` can cache data to boost performance. The following example cache data in a Redis cache.

```java
import com.github.ddth.cacheadapter.*;
import com.github.ddth.cacheadapter.cacheimpl.redis.*;
import com.github.ddth.commons.redis.*;
import com.github.ddth.cql.*;
import com.github.ddth.lucext.directory.cassandra.*;

// create a ICacheFactory
JedisConnector jc = new JedisConnector();
jc.setRedisHostsAndPorts("localhost:6379").seetRedisPassword("secret").init();
ICacheFactory cf = new RedisCacheFactory().setJedisConnector(jc).init();

// CassandraDirectory with caching enabled
SessionManager sm = new SessionManager();
sm.setDefaultHostsAndPorts("localhost")
    .setDefaultUsername("cassandra-user")
    .setDefaultPassword("cassandra-password)"
    .setDefaultKeyspace(null)
    .init();
Directory DIR = new CassandraDirectory(sm)
     .setKeyspace("my-keyspace")
     .setCacheFactory(cf).setCacheName("cachename")
     .init();
```

(See more about `ICacheManager` [here](https://github.com/DDTH/ddth-cache-adapter))


## License

See LICENSE.txt for details. Copyright (c) 2018 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.
