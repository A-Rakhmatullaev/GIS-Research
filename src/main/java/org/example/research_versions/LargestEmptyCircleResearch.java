package org.example.research_versions;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialOperator.KNNQuery;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Main;
import org.example.utilties.TempLogger;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LargestEmptyCircleResearch {
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
            if(datasetType.equals(DatasetType.USSchools))
                spatialRDD.setRawSpatialRDD(sedona.createDataset(USSchools(geometryFactory), Encoders.kryo(Point.class)).toJavaRDD());
            else if(datasetType.equals(DatasetType.RandomizedPoints))
                spatialRDD.setRawSpatialRDD(sedona.createDataset(randomLocations(geometryFactory), Encoders.kryo(Point.class)).toJavaRDD());
            else throw new RuntimeException("No dataset type!");

            mainStartTime = System.currentTimeMillis();

            spatialRDD.analyze();
            spatialRDD.spatialPartitioning(GridType.EQUALGRID);

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

            Geometry convexHull = finalHullDf.selectExpr("final_convex_hull").map((MapFunction<Row, Geometry>) row -> (Geometry) row.get(0), Encoders.kryo(Geometry.class)).first();

            System.out.println("Envelope: " + convexHull.getEnvelopeInternal());

            // Add Convex Hull vertices to the list of all points to form all points within which LEC is calculated
            ArrayList<Geometry> convexHullVertices = new ArrayList<>();
            for (Coordinate coordinate : convexHull.getCoordinates()) {
                convexHullVertices.add(geometryFactory.createPoint(coordinate));
            }
            List<Geometry> allPoints = new ArrayList<>(spatialRDD.rawSpatialRDD.collect());
            allPoints.addAll(convexHullVertices);
            Dataset<Geometry> allPointsDataset = sedona.createDataset(allPoints, Encoders.kryo(Geometry.class));
            SpatialRDD<Geometry> allPointsRDD = new SpatialRDD<>();
            allPointsRDD.setRawSpatialRDD(allPointsDataset.toJavaRDD());

            // Start creating voronoi polygons
            List<Coordinate> coordinateList = allPointsRDD.rawSpatialRDD.collect().stream().map(Geometry::getCoordinate).collect(Collectors.toList());
            Coordinate[] coordinates = new Coordinate[coordinateList.size()];
            coordinateList.toArray(coordinates);
            VoronoiDiagramBuilder voronoiDiagramBuilder = new VoronoiDiagramBuilder();
            voronoiDiagramBuilder.setSites(Arrays.asList(coordinates));
            voronoiDiagramBuilder.setClipEnvelope(convexHull.getEnvelopeInternal());

            GeometryCollection geometryCollection = (GeometryCollection) voronoiDiagramBuilder.getDiagram(geometryFactory);
            List<Polygon> voronoiPolygons = new ArrayList<>();
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                voronoiPolygons.add((Polygon) geometryCollection.getGeometryN(i));
            }

            // Uncomment if needed:
            // Find Voronoi vertices
//            List<Coordinate> voronoiVertices = new ArrayList<>();
//            for (Polygon polygon : voronoiPolygons) {
//                voronoiVertices.addAll(List.of(polygon.getCoordinates()));
//            }
//
//            System.out.println("Total # of Voronoi vertices: " + voronoiVertices.size());
//            System.out.println("Distinct # of Voronoi vertices: " + voronoiVertices.stream().distinct().count());
//

            // Obtain intersection points of Voronoi polygons' edges and Convex Hull + all other Voronoi vertices
            // This also ensures that all these vertices (points) are within Convex Hull
            List<Geometry> allVoronoiPolygonsAfterIntersectionWithConvexHull = new ArrayList<>();
            for (Polygon voronoiPolygon : voronoiPolygons) {
                for (int i = 0; i < voronoiPolygon.getNumGeometries(); i++) {
                    Geometry voronoiEdge = voronoiPolygon.getGeometryN(i);
                    for (int j = 0; j < convexHull.getNumGeometries(); j++) {
                        Geometry hullEdge = convexHull.getGeometryN(j);
                        Geometry intersection = voronoiEdge.intersection(hullEdge);
                        if (!intersection.isEmpty()) {
                            allVoronoiPolygonsAfterIntersectionWithConvexHull.add(intersection);
                        }
                    }
                }
            }

            // Uncomment if needed - it might be needed when you run python code because then you need Voronoi diagram layer to display, not only circle and points
            // generateVoronoiGeoJSON(allVoronoiPolygonsAfterIntersectionWithConvexHull, sedona);

            // Find All LEC Centers/Vertices
            List<Coordinate> lecCenters = new ArrayList<>();
            for (Geometry polygon : allVoronoiPolygonsAfterIntersectionWithConvexHull) {
                lecCenters.addAll(List.of(polygon.getCoordinates()));
            }

            System.out.println("Total # of Voronoi vertices: " + lecCenters.size());
            System.out.println("Distinct # of Voronoi vertices: " + lecCenters.stream().distinct().count());

            // Transform each LEC center/vertex from Coordinate to Geometry
            List<Geometry> lecCentersGeom = lecCenters.stream().map(geometryFactory::createPoint).collect(Collectors.toList());

            SpatialRDD<Geometry> lecCentersGeomRDD = new SpatialRDD<>();
            Dataset <Geometry> lecCentersDataset = sedona.createDataset(lecCentersGeom, Encoders.kryo(Geometry.class));
            lecCentersGeomRDD.setRawSpatialRDD(lecCentersDataset.toJavaRDD());
            lecCentersGeomRDD.analyze();
            lecCentersGeomRDD.spatialPartitioning(GridType.EQUALGRID);

            // Perform nearest neighbor search per partition
            JavaRDD<Tuple2<Geometry, Double>> lecCentersRDD = lecCentersGeomRDD.getRawSpatialRDD().mapPartitions(points -> {
                AtomicDouble maxRadius = new AtomicDouble(0);
                AtomicReference<Geometry> lecCenter = new AtomicReference<>();

                while (points.hasNext()){
                    Geometry vertex = points.next();
                    Geometry nearestPoint = null;
                    double nearestDistance = Double.MAX_VALUE;

                    // Compare the current vertex with every other point in the partition
                    for (Geometry other : allPoints) {
                        if (!vertex.equals(other)) {
                            double distance = vertex.distance(other);
                            if (distance < nearestDistance) {
                                nearestDistance = distance;
                                nearestPoint = other;
                            }
                        }
                    }

                    // Update maxRadius and lecCenter if the nearest distance is the largest so far
                    if (nearestPoint != null && nearestDistance > maxRadius.get()) {
                        maxRadius.set(nearestDistance);
                        lecCenter.set(vertex);
                    }
                }

                // Return the LEC center and the maximum radius from this partition
                if (lecCenter.get() != null) {
                    Tuple2<Geometry, Double> result = new Tuple2<>(lecCenter.get(), maxRadius.get());
                    return Collections.singleton(result).iterator();  // Wrap in an iterator
                } else
                    return Collections.emptyIterator();  // Return an empty iterator if no LEC center is found
            });

            lecCentersRDD = lecCentersRDD.cache();

            // Collect results from all partitions
            Tuple2<Geometry, Double> globalMaxLEC = lecCentersRDD.reduce((lec1, lec2) -> {
                // Compare the radii of the LEC centers from two partitions and keep the one with the larger radius
                return lec1._2 > lec2._2 ? lec1 : lec2;
            });

            System.out.println("Largest Empty Circle Center: " + globalMaxLEC._1);
            System.out.println("Largest Empty Circle Radius: " + globalMaxLEC._2);

            // Compute time it took for calculations
            mainEndTime = System.currentTimeMillis();
            System.out.println("Computation Elapsed time = " + (mainEndTime - mainStartTime));
            // Compute time it took in total
            totalEndTime = System.currentTimeMillis();
            System.out.println("Total Elapsed time = " + (totalEndTime - totalStartTime));

            // The rest of code is for saving values
            // Save results - all points
            //spatialRDD.flipCoordinates();
            //spatialRDD.saveAsGeoJSON(fileOutputLocation + "/all_points_result.json");

            // Create the largest empty circle for the dataset
//            Geometry lecCircle = geometryFactory.createPoint(lecCenter.get().getCoordinate()).buffer(maxRadius.get());
//            ArrayList <Geometry> l = new ArrayList<>();
//            l.add(lecCircle);
//            Dataset<Geometry> b = sedona.createDataset(l, Encoders.kryo(Geometry.class));
//            SpatialRDD <Geometry> result = new SpatialRDD<>();
//            result.setRawSpatialRDD(b.toJavaRDD());
//            result.flipCoordinates();

            // Save results - LEC
            //result.saveAsGeoJSON(fileOutputLocation + "/lec_result.json");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param datasetType
     * @implNote Must be called before all other operations
     */
    private void detectType(DatasetType datasetType) {
        if(datasetType.equals(DatasetType.USSchools)) {
            currentSetLocation = Main.resourceDir + "/us_private_schools";
        } else if (datasetType.equals(DatasetType.RandomizedPoints)) {
            currentSetLocation = Main.resourceDir + "/random_points";
        } else throw new RuntimeException("No dataset type!");
        fileInputLocation = currentSetLocation + "/input";
        fileOutputLocation = currentSetLocation + "/output/lec";
    }

    public enum DatasetType {
        USSchools,
        RandomizedPoints
    }

    private List<Point> USSchools(GeometryFactory geometryFactory) {
        String regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
        return readCoordinates(DatasetType.USSchools, fileInputLocation + "/school.csv", regex, geometryFactory);
    }

    private List<Point> randomLocations(GeometryFactory geometryFactory) {
        String regex = ",";
        return readCoordinates(DatasetType.RandomizedPoints, fileInputLocation + "/s.txt", regex, geometryFactory);
    }

    private List<Point> readCoordinates(DatasetType datasetType, String fileInputLocation, String regex, GeometryFactory geometryFactory) {
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

                coordinateXY = line.split(regex);
                if(coordinateXY.length != 0) {
                    float x;
                    float y;
                    if(datasetType.equals(DatasetType.USSchools)) {
                        x = Float.parseFloat(coordinateXY[1]);
                        y = Float.parseFloat(coordinateXY[0]);
                    } else if(datasetType.equals(DatasetType.RandomizedPoints)) {
                        x = Float.parseFloat(coordinateXY[0]);
                        y = Float.parseFloat(coordinateXY[1]);
                    } else throw new RuntimeException("No dataset type!");

                    if (x != Double.POSITIVE_INFINITY && y != Double.POSITIVE_INFINITY) {
                        coordinates.add(geometryFactory.createPoint(new Coordinate(x, y)));
                    }
                }
                i++;
            }
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }
        return coordinates;
    }

    private void generateVoronoiGeoJSON(List<Geometry> voronoiPolygons, SparkSession sedona) {
        // Convert Polygons to WKT strings
        WKTWriter wktWriter = new WKTWriter();
        List<String> wktList = new ArrayList<>();
        for (Geometry polygon : voronoiPolygons) {
            wktList.add(wktWriter.write(polygon));
        }
        Dataset<Geometry> wktDataset = sedona.createDataset(voronoiPolygons, Encoders.kryo(Geometry.class));

        SpatialRDD <Geometry> spatialRDD = new SpatialRDD<>();
        spatialRDD.setRawSpatialRDD(wktDataset.toJavaRDD());
        spatialRDD.flipCoordinates();
        // Save results - Voronoi Diagram
        spatialRDD.saveAsGeoJSON(fileOutputLocation + "/voronoi_diagram.json");
    }
}
