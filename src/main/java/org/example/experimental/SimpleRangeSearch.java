package org.example.experimental;

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Main;
import org.example.utilties.TempLogger;
import org.locationtech.jts.geom.*;

public class SimpleRangeSearch {
    String shapefileInputLocation = Main.resourceDir + "/nuclear";

    void start(SparkSession sedona) {
        TempLogger.log(shapefileInputLocation);

        try {
            SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                    JavaSparkContext.fromSparkContext(sedona.sparkContext()),
                    shapefileInputLocation
            );

            /**
             * Test values/coordinates (Format: Latitude, Longitude):
             *
             * @see https://www.mapcustomizer.com/map/rangeSearch1
             */
            Coordinate Khabarovsk = new Coordinate(48.4814, 135.0721);
            Coordinate Tokyo = new Coordinate(35.6764, 139.6500);
            Coordinate HongKong = new Coordinate(22.3193, 114.1694);
            Coordinate Tashkent = new Coordinate(41.3775, 64.5853);

            /**
             * Default format in nuclear.shp is:
             * Longitude, Latitude
             */

            // Flip coordinates to make them in (Latitude, Longitude) format
            spatialRDD.flipCoordinates();

            // Create a query window rectangle
            Envelope queryWindow = new Envelope(Tokyo, HongKong);
            //Envelope queryWindow = new Envelope(Khabarovsk, HongKong);
            //Envelope queryWindow = new Envelope(Tashkent, HongKong);

            // Create a predicate
            SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY;
            boolean usingIndex = false;

            // Query a SpatialRDD
            JavaRDD<Geometry> queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, spatialPredicate, usingIndex);

            // Set new value as value for queried SpatialRDD to convert it to DataFrame later
            spatialRDD.setRawSpatialRDD(queryResult);

            // Convert SpatialRDD with new value to DataFrame
            Dataset<Row> spatialQueryDf = Adapter.toDf(spatialRDD, sedona);

            // Show the result
            spatialQueryDf.show((int) spatialQueryDf.count(), false);
        } catch (Exception e) {
            System.out.println("Printing errors: "  + e);
            e.printStackTrace();
        }
    }
}
