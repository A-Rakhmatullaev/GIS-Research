package org.example.research_versions;

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Main;
import org.example.utilties.TempLogger;
import org.locationtech.jts.geom.Geometry;

public class ConvexHullResearch {
    String currentSetLocation = Main.resourceDir + "/nuclear";
    String fileInputLocation = currentSetLocation + "/input";
    String fileOutputLocation = currentSetLocation + "/output/convex_hull";

    void start(SparkSession sedona) {
        TempLogger.log(fileInputLocation);

        try {
            SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                    JavaSparkContext.fromSparkContext(sedona.sparkContext()),
                    fileInputLocation
            );

            // Flip coordinates to make them in (Latitude, Longitude) format
            spatialRDD.flipCoordinates();

            Dataset<Row> geometryDf = Adapter.toDf(spatialRDD, sedona);
            geometryDf.createTempView("geometryTable");

            Dataset<Row> aggregatedDf = sedona.sql("SELECT ST_Union_Aggr(geometryTable.geometry) AS agg FROM geometryTable");
            aggregatedDf.createTempView("aggregatedTable");

            Dataset<Row> convexHullDf = sedona
                    .sql("SELECT ST_ConvexHull(aggregatedTable.agg) AS convex_hull_geom FROM aggregatedTable")
                    .dropDuplicates();

            JavaRDD<Geometry> convexHullRawRDD = convexHullDf.selectExpr("convex_hull_geom").map((MapFunction<Row, Geometry>) row -> (Geometry) row.get(0), Encoders.kryo(Geometry.class)).toJavaRDD();
            spatialRDD.setRawSpatialRDD(convexHullRawRDD);
            spatialRDD.flipCoordinates();
            // Save results
            spatialRDD.saveAsGeoJSON(fileOutputLocation + "/convex_result.json");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
