package com.github.ddth.lucext.qnd.redis;

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

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.lucext.directory.redis.RedisDirectory;

import redis.clients.jedis.Jedis;

public class QndRedisIndexAndSearch extends BaseQndRedis {

    public static void main(String[] args) throws Exception {
        JedisConnector jc = getJedisConnector();
        try (Jedis jedis = jc.getJedis()) {
            jedis.flushAll();
        }

        try (RedisDirectory DIR = new RedisDirectory(jc)) {
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

        try (RedisDirectory DIR = new RedisDirectory(jc)) {
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
