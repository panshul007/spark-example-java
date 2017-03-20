package learning.spark.repository;

import learning.spark.models.TransactionRecord;
import org.springframework.data.repository.CrudRepository;

public interface TransactionRecordRepository  extends CrudRepository<TransactionRecord, Long>{

}
