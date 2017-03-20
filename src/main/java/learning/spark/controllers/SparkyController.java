package learning.spark.controllers;

import learning.spark.models.TransactionRecord;
import learning.spark.repository.TransactionRecordRepository;
import learning.spark.service.DataLoader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SparkyController {

    private final String sparkCluster = "spark://PanshulMBP.local:7077";
    private final String sparklocal = "local[4]";
    private final String tempWareHouse = "/Users/panshul/Development/sparkWarehouse";

    private JavaSparkContext sparkContext;
    private SparkSession sparkSession;

    @Autowired
    private TransactionRecordRepository trxRecordRepository;

    @Autowired
    private DataLoader dataLoader;

    @RequestMapping("/")
    public String index() {
        return "Greetings from Sparky Spring Boot";
    }

    @RequestMapping(method= RequestMethod.GET, path="/trx")
    public @ResponseBody Iterable<TransactionRecord> fetchPagedTransactionRecords() {
        return trxRecordRepository.findAll();
    }

    @RequestMapping(method=RequestMethod.GET, path = "/spark/trx/load-db")
    public String loadDBDataToSpark() {
        dataLoader.loadTransactions(getOrCreateSparkSession());
        return "data loaded";
    }

    @RequestMapping(method=RequestMethod.GET, path = "/spark/trx/load")
    public String loadDataToSpark() {
        dataLoader.loadTransactions(getOrCreateSparkContext());
        return "data loaded";
    }

    @RequestMapping(method=RequestMethod.GET, path = "/spark/movies/load")
    public String loaMovieDataToSpark() {
        Long moviesCount = dataLoader.loadMovies(getOrCreateSparkContext());
        return "data loaded: " + moviesCount;
    }

    private JavaSparkContext getOrCreateSparkContext() {
        if (this.sparkContext != null)
            return this.sparkContext;
        return new JavaSparkContext(getSparkConf());
    }

    private SparkSession getOrCreateSparkSession() {
        if (this.sparkSession != null)
            return this.sparkSession;
        return getSparkSession();
    }

    private SparkConf getSparkConf() {
        return new SparkConf()
                .setAppName("DataLoad")
                .setMaster(sparkCluster);
    }

    private SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("DataLoaderSQL")
                .master(sparkCluster)
                .config("spark.sql.warehouse.dir", tempWareHouse)
                .getOrCreate();
    }

}
