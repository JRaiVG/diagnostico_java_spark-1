package test.minsait.ttaa.datio;

import org.apache.spark.sql.SparkSession;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

public interface SparkSessionTestWrapper {

    SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();
}
