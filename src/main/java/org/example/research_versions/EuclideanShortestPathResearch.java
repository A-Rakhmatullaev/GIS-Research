package org.example.research_versions;

import org.apache.sedona.core.spatialRDD.LineStringRDD;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.Main;
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class EuclideanShortestPathResearch {
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
            PolygonRDD obstaclesRDD = generateObstacles(datasetType, geometryFactory, sedona);

            mainStartTime = System.currentTimeMillis();

            // Example points for the start and end of the shortest path
            Point startPoint = geometryFactory.createPoint(new Coordinate(16.0, 0.322));
            Point endPoint = geometryFactory.createPoint(new Coordinate(129.0, 0.2));

            // Collect all points to be used for visibility graph
            List<Point> points = new ArrayList<>();
            points.add(startPoint);
            points.add(endPoint);
            for (Polygon geom : obstaclesRDD.rawSpatialRDD.collect()) {
                points.addAll(Arrays.stream(geom.getCoordinates()).map(geometryFactory::createPoint).collect(Collectors.toList()));
            }

            JavaRDD<Point> pointJavaRDD = sedona.createDataset(points, Encoders.kryo(Point.class)).toJavaRDD();

            PointRDD pointsRDD = new PointRDD(pointJavaRDD);

            JavaPairRDD<Point, Iterable<Point>> visibilityGraphRDD = generateVisibilityGraph(pointsRDD, obstaclesRDD);

            List<Point> dijkstraShortestPath = dijkstraShortestPath(startPoint, endPoint, visibilityGraphRDD);

            // Compute time it took for calculations
            mainEndTime = System.currentTimeMillis();
            System.out.println("Computation Elapsed time = " + (mainEndTime - mainStartTime));
            // Compute time it took in total
            totalEndTime = System.currentTimeMillis();
            System.out.println("Total Elapsed time = " + (totalEndTime - totalStartTime));

//            // The rest of code is to save values
//            JavaRDD<Point> dijkstraRDD = sedona.createDataset(dijkstraShortestPath, Encoders.kryo(Point.class)).toJavaRDD();
//
//            PointRDD dijkstraPointRDD = new PointRDD(dijkstraRDD);
//
//            // Save the results - Code below can be commented if needed
//            saveResults(dijkstraPointRDD, dijkstraShortestPath, geometryFactory, sedona);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Point> dijkstraShortestPath(Point start, Point end, JavaPairRDD<Point, Iterable<Point>> visibilityGraphRDD) {
        Map<Point, Iterable<Point>> visibilityGraph = visibilityGraphRDD.collectAsMap();
        // Create queue of points to calculate the distance to
        PriorityBlockingQueue<Tuple2<Double, Point>> pq = new PriorityBlockingQueue<>(10, Comparator.comparingDouble(Tuple2::_1));
        pq.add(new Tuple2<>(0.0, start));

        ConcurrentMap<Point, Double> distances = new ConcurrentHashMap<>();
        distances.put(start, 0.0);

        ConcurrentMap<Point, Point> previous = new ConcurrentHashMap<>();
        Set<Point> visited = new HashSet<>();

        while (!pq.isEmpty()) {
            Tuple2<Double, Point> current = pq.poll();
            Point currentPoint = current._2();

            if (currentPoint.equals(end)) {
                List<Point> path = new ArrayList<>();
                for (Point at = end; at != null; at = previous.get(at)) {
                    path.add(at);
                }
                Collections.reverse(path);
                return path;
            }

            if (!visited.add(currentPoint)) continue;

            StreamSupport.stream(visibilityGraph.getOrDefault(currentPoint, Collections.emptyList()).spliterator(), true).forEach(neighbor -> {
                if (!visited.contains(neighbor)) {
                    double newDist = distances.get(currentPoint) + currentPoint.distance(neighbor);
                    if (newDist < distances.getOrDefault(neighbor, Double.MAX_VALUE)) {
                        distances.put(neighbor, newDist);
                        previous.put(neighbor, currentPoint);
                        pq.add(new Tuple2<>(newDist, neighbor));
                    }
                }
            });
        }
        return Collections.emptyList();
    }

    private JavaPairRDD<Point, Iterable<Point>> generateVisibilityGraph(PointRDD pointsRDD, PolygonRDD obstaclesRDD) {
        // Create a cartesian product of points with themselves to get all possible edges
        // + Filter out edges that cross any polygon
        List <Polygon> obstacles = obstaclesRDD.rawSpatialRDD.collect();
        JavaPairRDD<Point, Point> pairRDD = pointsRDD.rawSpatialRDD.cartesian(pointsRDD.rawSpatialRDD).filter((Function<Tuple2<Point, Point>, Boolean>) pair -> {
            if(pair._1.equals(pair._2)) return false;
            return arePointsVisible(pair._1, pair._2, obstacles);
        });

        // Group edges by their starting point
        return pairRDD.groupByKey();
    }

    private static boolean arePointsVisible(Point p1, Point p2, List <Polygon> obstacles) {
        LineString edge = p1.getFactory().createLineString(new Coordinate[]{p1.getCoordinate(), p2.getCoordinate()});
        for (Geometry polygon : obstacles) {
            if (polygon.crosses(edge)) {
                return false;
            }
        }
        return true;
    }

    private PolygonRDD generateObstacles(DatasetType datasetType, GeometryFactory geometryFactory, SparkSession sedona) throws IOException {
        List<Polygon> polygons = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(fileInputLocation))) {
            String line;
            String[] coords;
            int i = 0;
            // Adjust values from which to which obstacles you want to use
            int start;
            int end;
            if(datasetType.equals(DatasetType.ThreeHundred)) {
                start = 0;
                end = 299;
            } else if (datasetType.equals(DatasetType.FiveHundred)) {
                start = 0;
                end = 499;
            } else throw new RuntimeException("No dataset type!");

            while ((line = br.readLine()) != null) {
                if(i > start && i < end) {
                    coords = line.trim().split(":");
                    coords = Arrays.copyOf(coords, coords.length + 1);
                    coords[coords.length - 1] = coords[0];
                    List<Coordinate> points = new ArrayList<>();

                    for (String coord : coords) {
                        if (!coord.isEmpty()) {
                            String[] xy = coord.split(",");
                            double x = Double.parseDouble(xy[0]);
                            double y = Double.parseDouble(xy[1]);
                            points.add(new Coordinate(x, y));
                        }
                    }

                    if (!points.isEmpty()) {
                        Coordinate[] coordinatesArray = points.toArray(new Coordinate[0]);
                        Polygon polygon = geometryFactory.createPolygon(coordinatesArray);
                        polygons.add(polygon);
                    }
                }
                i++;
            }

            JavaRDD<Polygon> obstaclesRDD = sedona.createDataset(polygons, Encoders.kryo(Polygon.class)).toJavaRDD();
            // Save obstacles to display later
            //saveObstacles(obstaclesRDD);

            return new PolygonRDD(obstaclesRDD);
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IOException();
    }

    private void saveObstacles(JavaRDD<Polygon> obstaclesRDD) {
        PolygonRDD spatialRDD = new PolygonRDD(obstaclesRDD);
        spatialRDD.saveAsGeoJSON(fileOutputLocation + "/obstacles.json");
    }
    private void saveResults(PointRDD dijkstraPointRDD, List<Point> dijkstraShortestPath, GeometryFactory geometryFactory, SparkSession sedona) {
        dijkstraPointRDD.saveAsGeoJSON(fileOutputLocation + "/path_points.json");

        Coordinate[] dijkstraCoordinates = dijkstraShortestPath.stream().map(Point::getCoordinate).toArray(Coordinate[]::new);
        LineString lineString = geometryFactory.createLineString(dijkstraCoordinates);

        LineStringRDD lineStringRDD = new LineStringRDD(sedona.createDataset(Collections.singletonList(lineString), Encoders.kryo(LineString.class)).toJavaRDD());
        lineStringRDD.saveAsGeoJSON( fileOutputLocation + "/path_line.json");
    }

    /**
     * @param datasetType
     * @implNote Must be called before all other operations
     */
    private void detectType(DatasetType datasetType) {
        if(datasetType.equals(DatasetType.ThreeHundred)) {
            currentSetLocation = Main.resourceDir + "/300_obstacles";
            fileInputLocation = currentSetLocation + "/input/300Obstacles.txt";
        } else if (datasetType.equals(DatasetType.FiveHundred)) {
            currentSetLocation = Main.resourceDir + "/500_obstacles";
            fileInputLocation = currentSetLocation + "/input/500Obstacles.txt";
        } else throw new RuntimeException("No dataset type!");
        fileOutputLocation = currentSetLocation + "/output/esp";
    }

    public enum DatasetType {
        ThreeHundred,
        FiveHundred
    }
}
