package org.example.research_versions;

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Main;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConvexHullResearch {
    String currentSetLocation;
    String fileInputLocation;
    String fileOutputLocation;

    public void start(SparkSession sedona, DatasetType datasetType) {
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

            spatialRDD.analyze();
            spatialRDD.spatialPartitioning(GridType.QUADTREE);

            // Uncomment if you need to save all points
            //spatialRDD.saveAsGeoJSON(fileOutputLocation + "/all_points_result.json");

            // Compute convex hull for each partition
            JavaRDD<Geometry> convexHullPerPartitionRDD = spatialRDD.getRawSpatialRDD()
                    .mapPartitions(points -> {
                        List<Point> pointList = new ArrayList<>();
                        points.forEachRemaining(pointList::add);

                        // Create a MultiPoint geometry and calculate the convex hull for this partition
                        Geometry multiPoint = geometryFactory.createMultiPoint(pointList.toArray(new Point[0]));
                        return Collections.singleton(multiPoint.convexHull()).iterator();
                    });

            // Union the convex hulls from each partition and represent them in one dataset
            SpatialRDD<Geometry> convexHullRDD = new SpatialRDD<>();
            convexHullRDD.setRawSpatialRDD(convexHullPerPartitionRDD);
            Dataset<Row> hullDf = Adapter.toDf(convexHullRDD, sedona);

            hullDf.createOrReplaceTempView("hullTable");

            // Final aggregation of hulls to a single convex hull
            Dataset<Row> finalHullDf = sedona.sql(
                    "SELECT ST_ConvexHull(ST_Union_Aggr(geometry)) as final_convex_hull FROM hullTable");

            JavaRDD<Geometry> convexHullRawRDD = finalHullDf.selectExpr("final_convex_hull").map((MapFunction<Row, Geometry>) row -> (Geometry) row.get(0), Encoders.kryo(Geometry.class)).toJavaRDD();
            //SpatialRDD<Geometry> resultRDD = new SpatialRDD<>();
            //resultRDD.setRawSpatialRDD(convexHullRawRDD);
            //resultRDD.flipCoordinates();
            // Save results
            //resultRDD.saveAsGeoJSON(fileOutputLocation + "/convex_result.json");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Point> fireOccurrences(GeometryFactory geometryFactory) {
        return readCoordinates(RangeSearchResearch.DatasetType.Fire, fileInputLocation + "/fire.csv", geometryFactory);
    }

    private List<Point> crimeLocations(GeometryFactory geometryFactory) {
        return readCoordinates(RangeSearchResearch.DatasetType.Crime, fileInputLocation + "/crime.csv", geometryFactory);
    }

    private List<Point> readCoordinates(RangeSearchResearch.DatasetType datasetType, String fileInputLocation, GeometryFactory geometryFactory) {
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
                    if(datasetType.equals(RangeSearchResearch.DatasetType.Fire)) {
                        x = Double.parseDouble(coordinateXY[1]);
                        y = Double.parseDouble(coordinateXY[0]);
                        // This is needed because dataset definitely contains incorrect coordinates
                        if(x > 83 || x < -80) continue;
                    } else if(datasetType.equals(RangeSearchResearch.DatasetType.Crime)) {
                        x = Double.parseDouble(coordinateXY[0]);
                        y = Double.parseDouble(coordinateXY[1]);
                    } else throw new RuntimeException("No dataset type!");

                    if (x != Double.POSITIVE_INFINITY && y != Double.POSITIVE_INFINITY) {
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
        fileOutputLocation = currentSetLocation + "/output/convex_hull";
    }

    public enum DatasetType {
        Fire,
        Crime
    }
}
