import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os
import folium
import glob


dir_path = os.path.dirname(os.path.abspath(__file__))
#all_points_path = os.path.join(dir_path, 'nuclear', 'output', 'all_points_result.json', 'part-00000')
obstacles_files = glob.glob(os.path.join(dir_path, '300_obstacles', 'output', 'esp', 'obstacles.json', 'part-*'))
line_files = glob.glob(os.path.join(dir_path, '300_obstacles', 'output', 'esp', 'path_line.json', 'part-*'))
points_files = glob.glob(os.path.join(dir_path, '300_obstacles', 'output', 'esp', 'path_points.json', 'part-*'))

obstacles_gdf = gpd.GeoDataFrame()

for file in obstacles_files:
    gdf = gpd.read_file(file)
    obstacles_gdf = pd.concat([obstacles_gdf, gdf], ignore_index=True)

# Plot the polygons on a coordinate system
fig, ax = plt.subplots(figsize=(20, 20))
obstacles_gdf.plot(ax=ax, color='blue', edgecolor='black')

line_gdf = gpd.GeoDataFrame()

for file in line_files:
    gdf = gpd.read_file(file)
    line_gdf = pd.concat([line_gdf, gdf], ignore_index=True)

# Plot the polygons on a coordinate system
#fig, ax = plt.subplots(figsize=(20, 20))
line_gdf.plot(ax=ax, color='green', edgecolor='green')

points_gdf = gpd.GeoDataFrame()

for file in points_files:
    gdf = gpd.read_file(file)
    points_gdf = pd.concat([points_gdf, gdf], ignore_index=True)

# Plot the polygons on a coordinate system
#fig, ax = plt.subplots(figsize=(20, 20))
points_gdf.plot(ax=ax, color='red', edgecolor='yellow')

# Set the aspect of the plot to be auto
ax.set_aspect('auto')

# Add title and labels
ax.set_title('Euclidean Shortest Path')
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')

# Show the plot
plt.show()