package org.example;

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;

import java.util.Arrays;

public class SimpleExample {
    String shapefileInputLocation = Main.resourceDir + "/nuclear";

    void testStart(SparkSession sedona) {
        TempLogger.log(shapefileInputLocation);

        try {
            SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                    JavaSparkContext.fromSparkContext(sedona.sparkContext()),
                    shapefileInputLocation
            );
            //TempLogger.log("Field Names: \n");
            //spatialRDD.fieldNames.forEach(s -> TempLogger.log("Name: " + s));
            //TempLogger.log("\nPoints: \n");
            //spatialRDD.rawSpatialRDD.collect().forEach(TempLogger::log);

//            spatialRDD.rawSpatialRDD.foreach(obj -> {
//                TempLogger.log(obj.getUserData());
//            });

            /**
             * Default format in nuclear.shp is:
             * Longitude, Latitude
             */

            spatialRDD.flipCoordinates();
            Dataset<Row> spatialDf = Adapter.toDf(spatialRDD, sedona);
            //spatialDf.filter(new Column("Country").equalTo("Russia")).show((int) spatialDf.count(), false);
            spatialDf.show((int) spatialDf.count(), false);
        } catch (Exception e) {
            System.out.println("Printing errors: "  + e);
            e.printStackTrace();
        }
    }
}
