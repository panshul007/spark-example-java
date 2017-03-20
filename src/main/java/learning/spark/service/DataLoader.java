package learning.spark.service;


import learning.spark.models.TransactionRecord;
import learning.spark.repository.TransactionRecordRepository;
import learning.spark.utils.UuidConverter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

@Service
public class DataLoader {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private TransactionRecordRepository trxRecordRepository;

    public void loadTransactions(SparkSession session) {
        try {
            Map<String,String> jdbcOptions = new HashMap<String,String>();
            jdbcOptions.put("url", "jdbc:mysql://localhost:3306/t2");
//            jdbcOptions.put("driver", "com.mysql.jdbc.Driver");
            jdbcOptions.put("dbtable", "trx_rec");
            jdbcOptions.put("user", "root");
            jdbcOptions.put("password", "root");

            Dataset<Row> trxDataFrame = session.read().format("jdbc").options(jdbcOptions).load();

            System.out.println("data set loaded.");
            trxDataFrame.printSchema();
            trxDataFrame.cache();

            trxDataFrame.createOrReplaceTempView("trxTable");

            Dataset<Row> trxResolvedDataFrame = session.sql("Select ID, HEX(channel_id) as channel_id, HEX(merchant_id) as merchant_id, HEX(unique_id) as trx_id, type_id, tran_amt from trxTable");
            //trxResolvedDataFrame.show();

            System.out.println("MerchantId");
            trxResolvedDataFrame.groupBy("merchant_id").count().show();
            System.out.println("Channels");
            trxResolvedDataFrame.groupBy("channel_id").count().show();
            System.out.println("types");
            trxResolvedDataFrame.groupBy("type_id").count().show();
            System.out.println(trxResolvedDataFrame.count());

        } catch (Exception e) {
            logger.error("Error while loading from DB: ", e.getMessage());
            logger.error("Error while loading from DB: ", e);
        }
    }

    public void loadTransactions(JavaSparkContext context) {
        try {
            List<TransactionRecord> transactionRecordList = new ArrayList<>();
            Iterable<TransactionRecord> all = trxRecordRepository.findAll();
            all.forEach(trx -> transactionRecordList.add(trx));
            System.out.println("Trx array list ready: "+ transactionRecordList.size());
            JavaRDD<TransactionRecord> trxRecordRDD = context.parallelize(transactionRecordList, 4);
            System.out.println(trxRecordRDD.count());
            System.out.println("data frame loaded");
        }catch (Exception e) {
            logger.error("Error while loading transactions", e.getCause());
        }
    }

    public Long loadMovies(JavaSparkContext context) {
        Long count = 0L;
        try {
            JavaRDD<String> movieData = context.textFile("/Users/panshul/Development/sparkDataDump/ratings.csv", 4);
            count = movieData.count();
            System.out.println("db count: " + count);
            return count;
        } catch(Exception e) {
            System.out.println(e);
        }
        return count;
    }

    private void convertUUIDColumns (SparkSession session, Dataset<Row> trxDataFrame) {
        Dataset<Row> dataFrame = session.createDataFrame(trxDataFrame.javaRDD().map(row -> {
            return RowFactory.create(row.get(0),
                    new UuidConverter().convertToEntityAttribute((byte[])row.get(1)).toString(),
                    new UuidConverter().convertToEntityAttribute((byte[])row.get(2)).toString(),
                    row.get(3),
                    row.get(4),
                    row.get(5),
                    row.get(6),
                    row.get(7),
                    row.get(8),
                    row.get(9),
                    new UuidConverter().convertToEntityAttribute((byte[])row.get(10)).toString(),
                    row.get(11),
                    row.get(12),
                    row.get(13),
                    row.get(14),
                    row.get(15)
            );
        }), trxDataFrame.schema());
    }

    // root
// |-- ID: long (nullable = false)
// |-- channel_id: binary (nullable = true)
// |-- merchant_id: binary (nullable = true)
// |-- merch_trx_id: string (nullable = true)
// |-- merch_trx_ref: string (nullable = true)
// |-- processed: long (nullable = false)
// |-- requested: long (nullable = false)
// |-- short_id: long (nullable = false)
// |-- tran_amt: decimal(20,2) (nullable = true)
//            |-- type_id: integer (nullable = true)
// |-- unique_id: binary (nullable = true)
// |-- reference_id: long (nullable = true)
// |-- tran_curr_id: integer (nullable = true)
// |-- binding_snapshot_id: long (nullable = true)
// |-- invoiceId: string (nullable = true)
// |-- shopperId: string (nullable = true)

}
