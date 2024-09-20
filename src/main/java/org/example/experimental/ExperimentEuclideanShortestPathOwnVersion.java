package org.example.experimental;

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.LineStringRDD;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.example.Main;
import org.example.utilties.TempLogger;
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ExperimentEuclideanShortestPathOwnVersion {
    String shapefileInputLocation = Main.resourceDir + "/nuclear";

    void start(SparkSession sedona) {
        TempLogger.log(shapefileInputLocation);

        try {
            GeometryFactory geometryFactory = new GeometryFactory();

            SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                    JavaSparkContext.fromSparkContext(sedona.sparkContext()),
                    shapefileInputLocation
            );

            // Flip coordinates to make them in (Latitude, Longitude) format
            spatialRDD.flipCoordinates();

            Dataset<Row> geometryDf = Adapter.toDf(spatialRDD, sedona);

            // Example points for the start and end of the shortest path
            Point startPoint = new GeometryFactory().createPoint(new Coordinate(101.0, 0.6));
            Point endPoint = new GeometryFactory().createPoint(new Coordinate(129.0, 0.2));

            PolygonRDD obstaclesRDD = generateObstacles(geometryFactory, sedona);

            // Collect all points to be used for visibility graph
            List<Point> points = new ArrayList<>();
            points.add(startPoint);
            points.add(endPoint);
            for (Polygon geom : obstaclesRDD.rawSpatialRDD.collect()) {
                points.addAll(Arrays.stream(geom.getCoordinates()).map(geometryFactory::createPoint).collect(Collectors.toList()));
            }

            JavaRDD <Point> pointJavaRDD = sedona.createDataset(points, Encoders.kryo(Point.class)).toJavaRDD();

            PointRDD pointsRDD = new PointRDD(pointJavaRDD);

//            PointRDD pointsRDD = new PointRDD(obstaclesRDD.rawSpatialRDD.flatMap((FlatMapFunction<Polygon, Point>) obstacle -> {
//                List<Point> points = new ArrayList<>();
//                for (Coordinate coordinate : obstacle.getCoordinates()) {
//                    points.add(geometryFactory.createPoint(coordinate));
//                }
//                return points.iterator();
//            }));

            // TODO: Repartition points generation + check for duplicate points?

            JavaPairRDD <Point, Iterable<Point>> visibilityGraphRDD = generateVisibilityGraph(pointsRDD, obstaclesRDD);

            List<Point> dijkstraShortestPath = dijkstraShortestPath(startPoint, endPoint, visibilityGraphRDD);

            JavaRDD<Point> dijkstraRDD = sedona.createDataset(dijkstraShortestPath, Encoders.kryo(Point.class)).toJavaRDD();

            PointRDD dijkstraPointRDD = new PointRDD(dijkstraRDD);

            dijkstraPointRDD.saveAsGeoJSON(shapefileInputLocation + "/output/esp/path_points.json");

            Coordinate[] dijkstraCoordinates = dijkstraShortestPath.stream().map(Point::getCoordinate).toArray(Coordinate[]::new);
            LineString lineString = geometryFactory.createLineString(dijkstraCoordinates);

            LineStringRDD lineStringRDD = new LineStringRDD(sedona.createDataset(Collections.singletonList(lineString), Encoders.kryo(LineString.class)).toJavaRDD());
            lineStringRDD.saveAsGeoJSON(shapefileInputLocation + "/output/esp/path_line.json");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Point> dijkstraShortestPath(Point start, Point end, JavaPairRDD<Point, Iterable<Point>> visibilityGraphRDD) {
        // Convert visibility graph to Dijkstra's graph
        Map<Point, Iterable<Tuple2<Point, Double>>> dijkstraGraph = dijkstraGraph(visibilityGraphRDD);
        // Create distance table to track distance from start to each point and prev value: point, <distance to the point, previous point>
        Map<Point, Tuple2<Point, Double>> distanceTable = new HashMap<>();
        // Add starting point to distance table
        distanceTable.put(start, new Tuple2<>(null, 0.0));
        // Create set of all visited points
        Set<Point> visited = new HashSet<>();

        // Keep exploring Dijkstra's graph until you visit all the points
        while (true) {
            // Get the next point that has the least distance to the start and is not visited
            Map.Entry<Point, Tuple2<Point, Double>> minEntry = null;
            Double prevVal = Double.MAX_VALUE;
            for(Map.Entry<Point, Tuple2<Point, Double>> entry : distanceTable.entrySet()) {
                if(visited.contains(entry.getKey())) continue;
                if(entry.getValue()._2 < prevVal) {
                    prevVal = entry.getValue()._2;
                    minEntry = entry;
                }
            }

            // After finding the minEntry (if any), make sure it's not null, otherwise, it means all distances to all points were calculated
            if(minEntry == null) {
                break;
            }

            // For every neighbor of the node: Calculate the distance from start to neighbor
            // Also, add every neighbor to the distance table if the new distance is less than previous distance
            // logic: new distance < prev distance
            Point currentPoint = minEntry.getKey();
            // Distance and previous point
            Tuple2<Point, Double> currentPointTuple = minEntry.getValue();
            dijkstraGraph.get(currentPoint).forEach(pointDoubleTuple2 -> {
                Tuple2<Point, Double> neighborPointTuple = distanceTable.get(pointDoubleTuple2._1);
                if(neighborPointTuple == null) {
                    // If the point is not present in the distance table, add it with the value from Dijkstra's graph
                    // Here, pointDoubleTuple2._1 stores point, which is neighbor to currentPoint,
                    // and pointDoubleTuple2._2 stores distance from currentPoint to this neighbor
                    distanceTable.put(pointDoubleTuple2._1, new Tuple2<>(currentPoint, currentPointTuple._2 + pointDoubleTuple2._2()));
                } else {
                    // If the point is present in the distance table, then get its distance and compare with new distance
                    Double prevDistance = neighborPointTuple._2;
                    // New distance: distance from start to this point + distance from this point to its neighbor
                    Double newDistance = currentPointTuple._2 + pointDoubleTuple2._2;
                    // If prev distance is more than new distance, change nothing
                    // Else, assign new distance and new prev point to the distance table
                    if(newDistance < prevDistance) {
                        distanceTable.put(pointDoubleTuple2._1, new Tuple2<>(currentPoint, newDistance));
                    }
                }
            });

            if(minEntry.getKey().equals(end)) {
                // TODO: what if you stop here?
                System.out.println("End point has been found, and distance to it is: " + distanceTable.get(minEntry.getKey()));
            }
            visited.add(minEntry.getKey());
        }

        System.out.println("Min Distance: " + distanceTable.get(end));
        // Recreate the path
        List<Point> path = new ArrayList<>();
        while(true) {
            for (Point at = end; at != null; at = distanceTable.get(at)._1) {
                if(at == null) break;
                path.add(at);
            }
            Collections.reverse(path);
            break;
        }
        path.forEach(System.out::println);
        return path;
    }

    private Map<Point, Iterable<Tuple2<Point, Double>>> dijkstraGraph(JavaPairRDD<Point, Iterable<Point>> visibilityGraphRDD) {
        // Convert JavaPairRDD to a Map for Dijkstra's algorithm (may not be the best for very large graphs)
        Map<Point, Iterable<Point>> graph = visibilityGraphRDD.collectAsMap();
        // Create new graph that will have weighted edges
        Map<Point, Iterable<Tuple2<Point, Double>>> dijkstraGraph = new HashMap<>();
        // Find distance from each point to each of its neighbors (weights of edges)
        for(Map.Entry<Point, Iterable<Point>> entry : graph.entrySet()) {
            List <Tuple2<Point, Double>> tempWeights = new ArrayList<>();
            // Calculate distance between current point and each of its neighbors, and this is going to be a weight
            entry.getValue().forEach(neighbor -> {
                // TODO: error? Why does each neighbor get added twice?
                Double newDist = entry.getKey().distance(neighbor);
                tempWeights.add(new Tuple2<>(neighbor, newDist));
            });
            // Add weighted edges to Dijkstra's Graph
            dijkstraGraph.put(entry.getKey(), tempWeights);
        }
        return dijkstraGraph;
    }

    private JavaPairRDD<Point, Iterable<Point>> generateVisibilityGraph(PointRDD pointsRDD, PolygonRDD obstaclesRDD) {
        // Create a cartesian product of points with themselves to get all possible edges
        // + Filter out edges that cross any polygon
        List <Polygon> obstacles = obstaclesRDD.rawSpatialRDD.collect();
        // TODO: maybe repartition, and then compare part to all other points
        JavaPairRDD<Point, Point> pairRDD = pointsRDD.rawSpatialRDD.cartesian(pointsRDD.rawSpatialRDD).filter((Function <Tuple2<Point, Point>, Boolean>) pair -> {
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

    private PolygonRDD generateObstacles(GeometryFactory geometryFactory, SparkSession sedona) throws IOException {
        String filePath = Main.resourceDir + "/300Obstacles.txt";
        List<Polygon> polygons = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int i = 0;
            int start = 100;
            int end = 150;
            while ((line = br.readLine()) != null) {
                if(i > start && i < end) {
                    String[] coords = line.trim().split(":");
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
            //JavaRDD<Polygon> obstaclesRDD = obstaclesDataset.map((MapFunction<Polygon, Polygon>) polygon -> polygon, Encoders.kryo(Polygon.class)).toJavaRDD();
            PolygonRDD spatialRDD = new PolygonRDD(obstaclesRDD);
            spatialRDD.saveAsGeoJSON(shapefileInputLocation + "/output/esp/obstacles.json");
            return new PolygonRDD(obstaclesRDD);
            //spatialRDD.flipCoordinates();

        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IOException();
    }
}

