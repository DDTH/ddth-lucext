package com.github.ddth.lucext.qnd.redis;

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

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.lucext.directory.redis.RedisDirectory;

public class QndRedisSearchDirectory extends BaseQndRedis {

    public static void main(String[] args) throws Exception {
        JedisConnector jc = getJedisConnector();

        try (RedisDirectory DIR = new RedisDirectory(jc)) {
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
