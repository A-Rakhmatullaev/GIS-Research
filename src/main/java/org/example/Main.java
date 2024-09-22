package org.example;

import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.example.research_versions.ConvexHullResearch;
import org.example.research_versions.RangeSearchResearch;

public class Main {
    private static final String userDir = System.getProperty("user.dir");
    public static String resourceDir = userDir + "/src/resources/";

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

        //new RangeSearch().start(sedona);
        //new ConvexHull().start(sedona);
        //new LargestEmptyCircle().start(sedona);
        //new EuclideanShortestPath().start(sedona);

        //new RangeSearchResearch().start(sedona, RangeSearchResearch.DatasetType.Fire);
        new ConvexHullResearch().start(sedona, ConvexHullResearch.DatasetType.Fire);

        SedonaVizRegistrator.dropAll(sedona);

        sedona.close();
        sedona.stop();

        sparkSessionConfig.stop();
        sparkSessionConfig.close();
    }
}