package com.github.ddth.lucext.qnd.cassandra;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.lucext.directory.cassandra.CassandraDirectory;

import ch.qos.logback.classic.Level;

public class QndCassandraIndexAndSearch extends BaseQndCassandra {

    public static void main(String[] args) throws Exception {
        initLoggers(Level.INFO);

        ProgrammaticDriverConfigLoaderBuilder dclBuilder = DriverConfigLoader.programmaticBuilder();
        dclBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra");
        try (SessionManager sm = getSessionManager()) {
            sm.setConfigLoader(dclBuilder.build());
            sm.setDefaultHostsAndPorts("localhost");
            sm.init();
            
            sm.execute(
                    "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION={'class' : 'SimpleStrategy', 'replication_factor' : 1}");
            sm.execute("DROP TABLE IF EXISTS test." + CassandraDirectory.DEFAULT_TBL_FILEDATA);
            sm.execute("DROP TABLE IF EXISTS test." + CassandraDirectory.DEFAULT_TBL_METADATA);

            sm.execute("CREATE TABLE test." + CassandraDirectory.DEFAULT_TBL_METADATA
                    + " (name VARCHAR, size BIGINT, id VARCHAR, PRIMARY KEY (name))");
            sm.execute("CREATE TABLE test." + CassandraDirectory.DEFAULT_TBL_FILEDATA
                    + " (id VARCHAR, blocknum INT, blockdata BLOB, PRIMARY KEY (id, blocknum))");
            Thread.sleep(1000);

            ICacheFactory cf = getCacheFactory(true);

            try (CassandraDirectory DIR = new CassandraDirectory(sm)) {
                DIR.setKeyspace("test").setCacheFactory(cf).setCacheName("lucext");
                DIR.init();

                long t1 = System.currentTimeMillis();

                Analyzer analyzer = new StandardAnalyzer();
                IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

                IndexWriter iw = new IndexWriter(DIR, iwc);
                Document doc = new Document();
                doc.add(new StringField("id", "thanhnb", Field.Store.YES));
                doc.add(new TextField("name", "Nguyen Ba Thanh", Field.Store.NO));
                iw.updateDocument(new Term("id", "thanhnb"), doc);

                iw.commit();
                iw.close();

                long t2 = System.currentTimeMillis();
                System.out.println("Finished indexing in " + (t2 - t1) / 1000.0 + " sec");
            }

            try (CassandraDirectory DIR = new CassandraDirectory(sm)) {
                DIR.setKeyspace("test").setCacheFactory(cf).setCacheName("lucext");
                DIR.init();

                long t1 = System.currentTimeMillis();

                IndexReader ir = DirectoryReader.open(DIR);
                IndexSearcher is = new IndexSearcher(ir);

                Analyzer analyzer = new StandardAnalyzer();
                QueryParser parser = new QueryParser(null, analyzer);
                Query q = parser.parse("id:thanhnb");
                TopDocs result = is.search(q, 10);
                System.out.println("Hits:" + result.totalHits);
                for (ScoreDoc sDoc : result.scoreDocs) {
                    int docId = sDoc.doc;
                    Document doc = is.doc(docId);
                    System.out.println(doc);
                }

                ir.close();

                long t2 = System.currentTimeMillis();
                System.out.println("Finished searching in " + (t2 - t1) / 1000.0 + " sec");
            }
        }
    }
}
