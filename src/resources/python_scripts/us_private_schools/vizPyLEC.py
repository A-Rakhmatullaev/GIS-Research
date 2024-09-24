import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os
import folium
import glob


dir_path = os.path.dirname(os.path.abspath(__file__))
all_points_files = glob.glob(os.path.join(dir_path, '..', '..', 'us_private_schools', 'output', 'lec', 'all_points_result.json', 'part-*'))
lec_files = glob.glob(os.path.join(dir_path, '..', '..', 'us_private_schools', 'output', 'lec', 'lec_result.json', 'part-*'))
# List all GeoJSON voronoi polygons files
#voronoi_files = glob.glob(os.path.join(dir_path, 'nuclear', 'output', 'lec', 'voronoi_diagram.json', 'part-*'))

# points_gdf = gpd.GeoDataFrame()

# for file in all_points_files:
#     gdf = gpd.read_file(file)
#     points_gdf = pd.concat([points_gdf, gdf], ignore_index=True)

lec_circle_gdf = gpd.GeoDataFrame()

for file in lec_files:
    gdf = gpd.read_file(file)
    lec_circle_gdf = pd.concat([lec_circle_gdf, gdf], ignore_index=True)

#voronoi_gdf = gpd.GeoDataFrame()
# Read each part and concatenate them into a single GeoDataFrame
# for file in voronoi_files:
#     gdf = gpd.read_file(file)
#     voronoi_gdf = pd.concat([voronoi_gdf, gdf], ignore_index=True)

# Create a Folium map centered around the mean of all points
m = folium.Map(location=[0, 0], zoom_start=2)

# Add all the boundary points to the map
#folium.GeoJson(points_gdf, name="Points").add_to(m)
# Add the largest empty circle to the map
folium.GeoJson(lec_circle_gdf.to_json(), name="Largest Empty Circle", style_function=lambda x: {
    'fillColor': '#ff7800',
    'color': '#ff7800',
    'weight': 2,
    'fillOpacity': 0.2,
}).add_to(m)
# Add all the Voronoi polygons fitted to the Convex Hull
#folium.GeoJson(voronoi_gdf, name="Voronoi").add_to(m)

# Add points to the map
#folium.GeoJson(points_gdf.to_json(), name="Points").add_to(m)

# Add layer control to toggle layers on and off
folium.LayerControl().add_to(m)

#m.save("NuclearLECMap.html")
m.show_in_browser()