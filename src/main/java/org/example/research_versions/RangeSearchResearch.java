package org.example.research_versions;

import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialOperator.SpatialPredicate;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.Main;
import org.locationtech.jts.geom.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RangeSearchResearch {
    String currentSetLocation;
    String fileInputLocation;
    String fileOutputLocation;
    long totalStartTime = 0;
    long totalEndTime = 0;
    long mainStartTime = 0;
    long mainEndTime = 0;

    public void start(SparkSession sedona, DatasetType datasetType) {
        totalStartTime = System.currentTimeMillis();
        // Must be called before all other operations
        detectType(datasetType);

        try {
            GeometryFactory geometryFactory = new GeometryFactory();

            SpatialRDD<Point> spatialRDD = new SpatialRDD<>();
            if(datasetType.equals(DatasetType.Fire))
                spatialRDD.setRawSpatialRDD(sedona.createDataset(fireOccurrences(geometryFactory), Encoders.kryo(Point.class)).toJavaRDD());
            else if(datasetType.equals(DatasetType.Crime))
                spatialRDD.setRawSpatialRDD(sedona.createDataset(crimeLocations(geometryFactory), Encoders.kryo(Point.class)).toJavaRDD());
            else throw new RuntimeException("No dataset type!");

            mainStartTime = System.currentTimeMillis();

            // Shahruz's coordinates
            Envelope queryWindow = new Envelope(new Coordinate(-59.202464, -127.663167), new Coordinate(49.472737, 23.8647));

            // Create a predicate
            SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY;
            boolean usingIndex = false;

            // Query a SpatialRDD
            JavaRDD<Point> queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, queryWindow, spatialPredicate, usingIndex);
            queryResult.collect();

            // Compute time it took for calculations
            mainEndTime = System.currentTimeMillis();
            System.out.println("Computation Elapsed time= " + (mainEndTime - mainStartTime));
            // Compute time it took in total
            totalEndTime = System.currentTimeMillis();
            System.out.println("Total Elapsed time = " + (totalEndTime - totalStartTime));
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
                        if(x > 83 || x < -80) continue;
                    } else if(datasetType.equals(DatasetType.Crime)) {
                        x = Double.parseDouble(coordinateXY[0]);
                        y = Double.parseDouble(coordinateXY[1]);
                    } else throw new RuntimeException("No dataset type!");

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

    /**
     * @param datasetType
     * @implNote Must be called before all other operations
     */
    private void detectType(DatasetType datasetType) {
        if(datasetType.equals(DatasetType.Fire)) {
            currentSetLocation = Main.resourceDir + "/fire_occurrences";
        } else if (datasetType.equals(DatasetType.Crime)) {
            currentSetLocation = Main.resourceDir + "/crime_locations";
        } else throw new RuntimeException("No dataset type!");
        fileInputLocation = currentSetLocation + "/input";
        fileOutputLocation = currentSetLocation + "/output/range_search";
    }

    public enum DatasetType {
        Fire,
        Crime
    }
}
