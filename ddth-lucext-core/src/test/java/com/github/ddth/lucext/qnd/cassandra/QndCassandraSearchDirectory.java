package com.github.ddth.lucext.qnd.cassandra;

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

import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.lucext.directory.cassandra.CassandraDirectory;

import ch.qos.logback.classic.Level;

public class QndCassandraSearchDirectory extends BaseQndCassandra {

    public static void main(String[] args) throws Exception {
        initLoggers(Level.INFO);

        ICacheFactory cf = getCacheFactory(false);

        try (SessionManager sm = getSessionManager()) {
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
                System.out.println("Finished indexing in " + (t2 - t1) / 1000.0 + " sec");
            }
        }
    }
}
