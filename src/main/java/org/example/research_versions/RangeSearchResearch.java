package org.example.research_versions;

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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

public class RangeSearchResearch {
    String currentSetLocation = Main.resourceDir + "/nuclear";
    String fileInputLocation = currentSetLocation + "/input";
    String fileOutputLocation = currentSetLocation + "/output/range_search";

    void start(SparkSession sedona) {
        TempLogger.log(fileInputLocation);

        try {
            SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                    JavaSparkContext.fromSparkContext(sedona.sparkContext()),
                    fileInputLocation
            );

            /**
             * Test values/coordinates (Format: Latitude, Longitude)
             */
            Coordinate Khabarovsk = new Coordinate(48.4826923, 135.0835877);
            Coordinate Tokyo = new Coordinate(35.6821936, 139.762221);
            Coordinate HongKong = new Coordinate(22.2793278, 114.1628131);
            Coordinate Tashkent = new Coordinate(41.3123363, 69.2787079);
            Coordinate Mumbai = new Coordinate(19.0815772, 72.8866275);

            Coordinate Paris = new Coordinate(48.8534951, 2.3483915);
            Coordinate Madrid = new Coordinate(40.4167047, -3.7035825);
            Coordinate Istanbul = new Coordinate(41.0063811, 28.9758715);
            Coordinate Moscow = new Coordinate(55.6255781, 37.6063916);
            Coordinate Hamburg = new Coordinate(53.5503071, 10.0006714);

            /**
             * Default format in nuclear.shp is:
             * Longitude, Latitude
             */

            // Flip coordinates to make them in (Latitude, Longitude) format
            spatialRDD.flipCoordinates();

            // Create a custom quadrilateral query window
//            GeometryFactory geometryFactory = new GeometryFactory();
//            Coordinate[] coordinates = new Coordinate[6];
//            coordinates[0] = Khabarovsk;
//            coordinates[1] = Tokyo;
//            coordinates[2] = HongKong;
//            coordinates[3] = Tashkent;
//            coordinates[4] = Mumbai;
//            coordinates[5] = coordinates[0]; // The last coordinate is the same as the first coordinate in order to compose a closed ring
//            Polygon polygonQueryWindow = geometryFactory.createPolygon(coordinates);

            // Create a custom quadrilateral query window
            GeometryFactory geometryFactory = new GeometryFactory();
            Coordinate[] coordinates = new Coordinate[6];
            coordinates[0] = Paris;
            coordinates[1] = Madrid;
            coordinates[2] = Istanbul;
            coordinates[3] = Moscow;
            coordinates[4] = Hamburg;
            coordinates[5] = coordinates[0]; // The last coordinate is the same as the first coordinate in order to compose a closed ring
            Polygon polygonQueryWindow = geometryFactory.createPolygon(coordinates);

            // Create a predicate
            SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY;
            boolean usingIndex = false;

            // Query a SpatialRDD
            JavaRDD<Geometry> queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, polygonQueryWindow, spatialPredicate, usingIndex);

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
