package com.github.ddth.lucext.qnd.cassandra;

import ch.qos.logback.classic.Level;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.lucext.directory.cassandra.CassandraDirectory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

public class QndCassandraSearchDirectory extends BaseQndCassandra {

    public static void main(String[] args) throws Exception {
        initLoggers(Level.INFO);

        ICacheFactory cf = getCacheFactory(false);

        ProgrammaticDriverConfigLoaderBuilder dclBuilder = DriverConfigLoader.programmaticBuilder();
        dclBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "cassandra")
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "cassandra");
        try (SessionManager sm = getSessionManager()) {
            sm.setConfigLoader(dclBuilder.build());
            sm.setDefaultHostsAndPorts("localhost");

            try (CassandraDirectory DIR = new CassandraDirectory(sm)) {
                DIR.setKeyspace("test").setCacheFactory(cf).setCacheName("lucext");
                DIR.init();

                long t1 = System.currentTimeMillis();

                IndexReader ir = DirectoryReader.open(DIR);
                IndexSearcher is = new IndexSearcher(ir);

                String[] terms = { "lucene", "redis", "cassandra" };
                for (String term : terms) {
                    Analyzer analyzer = new StandardAnalyzer();
                    QueryParser parser = new QueryParser(null, analyzer);
                    Query q = parser.parse("contents:" + term);
                    TopDocs result = is.search(q, 10);
                    System.out.println("Search for [" + term + "], hits:" + result.totalHits);
                    for (ScoreDoc sDoc : result.scoreDocs) {
                        int docId = sDoc.doc;
                        Document doc = is.doc(docId);
                        System.out.println(doc);
                    }
                }

                ir.close();

                long t2 = System.currentTimeMillis();
                System.out.println("Finished searching in " + (t2 - t1) / 1000.0 + " sec");
            }
        }
    }
}
