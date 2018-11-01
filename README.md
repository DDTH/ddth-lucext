[![Build Status](https://travis-ci.org/DDTH/ddth-lucext.svg?branch=master)](https://travis-ci.org/DDTH/ddth-lucext)

# ddth-lucext

DDTH's utilities and extensions for [Apache Lucene](http://lucene.apache.org).

Project home:
[https://github.com/DDTH/ddth-lucext](https://github.com/DDTH/ddth-lucext)


## Installation

Latest release version: `0.2.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency: if only a sub-set of `ddth-lucext` functionality is used, choose the corresponding
dependency artifact(s) to reduce the number of unused jar files.

*ddth-lucext-core*: (not much functionality at the moment)

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-lucext-core</artifactId>
    <version>0.2.0</version>
</dependency>
```

*ddth-lucext-cassandra*: include *ddth-lucext-core* and [Apache's Cassandra](http://cassandra.apache.org) dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-lucext-cassandra</artifactId>
    <version>0.2.0</version>
    <type>pom</type>
</dependency>
```

*ddth-lucext-redis*: include *ddth-lucext-core* and [Redis](https://redis.io) dependencies.

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-lucext-redis</artifactId>
    <version>0.2.0</version>
    <type>pom</type>
</dependency>
```


## Usage

### `IndexManager`

Help to manage index-objects (`IndexWriter`, `IndexSearcher` and `DirectoryReader`) associated with a `Directory`.

```java
// create an IndexManager instance
IndexManager indexManager = new IndexManager(directory);

// customize the IndexManager
indexManager.setIndexWriterConfig(iwc)
    .setScheduledExecutorService(ses)                    //supply a custom ScheduledExecutorService for background jobs
    .setBackgroundRefreshIndexSearcherPeriodMs(10000)    //automatically refresh DirectoryReader and IndexSearcher per 10 seconds
    .setBackgroundCommitIndexPeriodMs(1000)              //automatically call IndexWriter.commit() per 1 second
    .setNrtIndexSearcher(true)                           //enable near-real-time IndexSearcher
    ;

// remember to initialize the IndexManager
indexManager.init();
```

From this point, application obtains `IndexWriter`, `IndexSearcher` and `DirectoryReader` and works with them as usual.

```java
IndexWriter indexWriter = indexManager.getIndexWriter();
indexWriter.addDocument(...);
indexWriter.updateDocument(...);
indexWriter.removeDocument(...);
indexWriter.commit();
// (optionally) notify IndexManager that the index has changed due to document adding/removing/updating
//indexManager.markIndexChanged();

IndexSearcher indexSearcher = indexManager.getIndexSearcher();
indexSearcher.search(...);
```

Finally, do not forget to close the `IndexManager` when done:

```java
indexManager.destroy(); // or indexManager.close();
```

Notes:

- Do not close the obtained `IndexWriter` or `DirectoryReader`! `IndexManager.close()` will take care of closing those instances.
- Application is free to call `IndexWriter.commit()`. In most cases, however, let `IndexManager` do that in the background: `IndexManager.setBackgroundCommitIndexPeriodMs(1000)` should be sufficient.
- In near-real-time mode (which is turned on by default), `IndexManager.getDirectoryReader()` and `IndexManager.getIndexSeacher()` always return
the most up-to-date instances. If near-real-time mode is too costly for application (which is a rare case, however), application can turn
off near-real-time mode (`IndexManager.setNrtIndexSearcher(false)`) and enable background refresh of `IndexSearcher` (and `DirectoryReader`) via
`IndexManager.setBackgroundRefreshIndexSearcherPeriodMs(...)`.
- Warning: if both _near-real-time mode_ and _background IndexSeacher refresh_ are turned off, all index changes (document added/deleted/updated)
occurred after `IndexManager.init()` is called will not be read.
- After `IndexManager.init()` is invoked:
  - `setIndexWriterConfig(IndexWriterConfig)` will NOT take effect and a warning message will be logged.
  - `setScheduledExecutorService(ScheduledExecutorService)` will NOT take effect and a warning message will be logged.
  - `setBackgroundRefreshIndexSearcherPeriodMs(long)` will take effect on-the-fly.
  - `setBackgroundCommitIndexPeriodMs(long)` will take effect on-the-fly.
  - `setNrtIndexSearcher(boolean)` will take effect on-the-fly.


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
