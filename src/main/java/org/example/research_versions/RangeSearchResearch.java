package org.example.research_versions;

import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Main;
import org.example.utilties.TempLogger;
import org.locationtech.jts.geom.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RangeSearchResearch {
    // Chose one set depending on what you want
    //String currentSetLocation = Main.resourceDir + "/fire_occurrences";
    String currentSetLocation = Main.resourceDir + "/crime_locations";
    String fileInputLocation = currentSetLocation + "/input";
    String fileOutputLocation = currentSetLocation + "/output/range_search";

    public void start(SparkSession sedona) {
        TempLogger.log(fileInputLocation);

        try {
            GeometryFactory geometryFactory = new GeometryFactory();

            SpatialRDD<Point> spatialRDD = new SpatialRDD<>();
            // Choose one depending on what you want
            //spatialRDD.setRawSpatialRDD(sedona.createDataset(fireOccurrences(geometryFactory), Encoders.kryo(Point.class)).toJavaRDD());
            spatialRDD.setRawSpatialRDD(sedona.createDataset(crimeLocations(geometryFactory), Encoders.kryo(Point.class)).toJavaRDD());

            // Create a custom quadrilateral query window
//            Coordinate NewYork = new Coordinate(40.741895, -73.989308);
//            Coordinate Miami = new Coordinate(25.7741728, -80.19362);
//            Coordinate MexicoCity = new Coordinate(19.4326296, -99.1331785);
//            Coordinate LosAngeles = new Coordinate(34.0536909, -118.242766);
//            Coordinate Seattle = new Coordinate(47.6038321, -122.330062);
//
//            Coordinate[] coordinates = new Coordinate[6];
//            coordinates[0] = NewYork;
//            coordinates[1] = Miami;
//            coordinates[2] = MexicoCity;
//            coordinates[3] = LosAngeles;
//            coordinates[4] = Seattle;
//            coordinates[5] = coordinates[0]; // The last coordinate is the same as the first coordinate in order to compose a closed ring
//            Polygon queryWindow = geometryFactory.createPolygon(coordinates);

            // Shahruz's coordinates
            Envelope queryWindow = new Envelope(new Coordinate(-59.202464, -127.663167), new Coordinate(49.472737, 23.8647));

            // Create a predicate
            SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY;
            boolean usingIndex = false;

            // Query a SpatialRDD
            JavaRDD<Point> queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, spatialPredicate, usingIndex);

            // Set new value as value for queried SpatialRDD to convert it to DataFrame later
            spatialRDD.setRawSpatialRDD(queryResult);

            // Code below is to show the results
            // Convert SpatialRDD with new value to DataFrame
            //Dataset<Row> spatialQueryDf = Adapter.toDf(spatialRDD, sedona);

            // Show the result
            //spatialQueryDf.show((int) spatialQueryDf.count(), false);

            // Save the result
            //spatialRDD.saveAsGeoJSON(fileOutputLocation + "/points_in_range.json");
        } catch (Exception e) {
            System.out.println("Printing errors: "  + e);
            e.printStackTrace();
        }
    }

    private List<Point> fireOccurrences(GeometryFactory geometryFactory) {
        return readCoordinates(DatasetType.Fire, fileInputLocation + "/fire.csv", geometryFactory);
    }

    private List<Point> crimeLocations(GeometryFactory geometryFactory) {
        return readCoordinates(DatasetType.Crime, fileInputLocation + "/crime.csv", geometryFactory);
    }

    private List<Point> readCoordinates(DatasetType datasetType, String fileInputLocation, GeometryFactory geometryFactory) {
        ArrayList<Point> coordinates = new ArrayList<>();
        // Read CSV file
        try (BufferedReader br = new BufferedReader(new FileReader(fileInputLocation))) {
            String line;
            int i = 0;
            String[] coordinateXY;


            while ((line = br.readLine()) != null) {
                // Skip the first line, as it contains text that ruins the logic
                if (i == 0) {
                    i += 1;
                    continue;
                }
                coordinateXY = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                if(coordinateXY.length != 0) {
                    double x;
                    double y;
                    if(datasetType.equals(DatasetType.Fire)) {
                        x = Double.parseDouble(coordinateXY[1]);
                        y = Double.parseDouble(coordinateXY[0]);
                    } else {
                        x = Double.parseDouble(coordinateXY[0]);
                        y = Double.parseDouble(coordinateXY[1]);
                    }

                    if (x != Double.POSITIVE_INFINITY && y != Double.POSITIVE_INFINITY) {
//                        System.out.println("X: " + x);
//                        System.out.println("Y: " + y);
                        coordinates.add(geometryFactory.createPoint(new Coordinate(x, y)));
                    }
                    i += 1;
                }
            }
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
        return coordinates;
    }

    private enum DatasetType {
        Fire,
        Crime
    }
}
