package org.example;

import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;

public class Main {
    private static final String userDir = System.getProperty("user.dir");
    static String resourceDir = userDir + "/src/resources/";

    public static void main(String[] args) {
        SparkSession sparkSessionConfig = SedonaContext
                .builder()
                .appName("Sedona-SQL-Demo")
                .master("local[*]")
                .config("spark.kryo.registrator", SedonaKryoRegistrator.class.getName())
                .config("spark.serializer", KryoSerializer.class.getName())
                .config("spark.driver.extraJavaOptions", "-Dsedona.global.charset=utf8")
                .config("spark.executor.extraJavaOptions", "-Dsedona.global.charset=utf8")
                .getOrCreate();

        SparkSession sedona = SedonaContext.create(sparkSessionConfig);

        SedonaVizRegistrator.registerAll(sedona);


        //new SimpleExample().testStart(sedona);
        //new SimpleRangeSearch().start(sedona);
        //new RangeSearch().start(sedona);
        //new ExperimentConvexHull().start(sedona);
        new ConvexHull().start(sedona);


        SedonaVizRegistrator.dropAll(sedona);

        sedona.close();
        sedona.stop();

        sparkSessionConfig.stop();
        sparkSessionConfig.close();
    }
}