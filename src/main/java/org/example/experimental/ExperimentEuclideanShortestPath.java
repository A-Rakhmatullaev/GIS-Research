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

public class ExperimentEuclideanShortestPath {
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
            Point startPoint = new GeometryFactory().createPoint(new Coordinate(148.0, 0.754));
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
        Map<Point, Iterable<Point>> visibilityGraph = visibilityGraphRDD.collectAsMap();
        // Create queue of points to calculate the distance to
        PriorityQueue<Tuple2<Double, Point>> pq = new PriorityQueue<>(Comparator.comparingDouble(Tuple2::_1));
        pq.add(new Tuple2<>(0.0, start));

        Map<Point, Double> distances = new HashMap<>();
        distances.put(start, 0.0);

        Map<Point, Point> previous = new HashMap<>();
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

            for (Point neighbor : visibilityGraph.getOrDefault(currentPoint, Collections.emptyList())) {
                if (visited.contains(neighbor)) continue;

                double newDist = distances.get(currentPoint) + currentPoint.distance(neighbor);
                if (newDist < distances.getOrDefault(neighbor, Double.MAX_VALUE)) {
                    distances.put(neighbor, newDist);
                    previous.put(neighbor, currentPoint);
                    pq.add(new Tuple2<>(newDist, neighbor));
                }
            }
        }
        return Collections.emptyList();
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

