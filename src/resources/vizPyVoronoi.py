import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os
import folium
import glob

# !!!!! This is code that display voronoi diagram only
dir_path = os.path.dirname(os.path.abspath(__file__))

# List all GeoJSON part files
files = glob.glob(os.path.join(dir_path, 'nuclear', 'output', 'lec', 'voronoi_diagram.json', 'part-*'))

print(files)

# Initialize an empty GeoDataFrame
merged_gdf = gpd.GeoDataFrame()

# Read each part and concatenate them into a single GeoDataFrame
for file in files:
    gdf = gpd.read_file(file)
    merged_gdf = pd.concat([merged_gdf, gdf], ignore_index=True)

# ! - Can be coomented - Add an ID column if not present
if 'id' not in merged_gdf.columns:
    merged_gdf['id'] = range(1, len(merged_gdf) + 1)

# Save the merged GeoDataFrame to a single GeoJSON file (optional)
#merged_gdf.to_file(os.path.join(dir_path, 'nuclear', 'output', 'voronoi_result.json', 'merged_voronoi_result.json'), driver='GeoJSON')

# Create a Folium map centered around the mean of all points
m = folium.Map(location=[0, 0], zoom_start=2)

# ! - Can be commented - Add GeoJSON data to the map with popups
for _, row in merged_gdf.iterrows():
    # Create a polygon with a popup
    geo_json = folium.GeoJson(
        data=row.geometry.__geo_interface__,
        style_function=lambda x: {
            'fillColor': '#ffffff',
            'color': '#000000',
            'weight': 1.5,
            'fillOpacity': 0.5,
        }
    )
    folium.Popup(f"ID: {row['id']}").add_to(geo_json)
    geo_json.add_to(m)

# Add GeoJSON data to the map
#folium.GeoJson(merged_gdf).add_to(m)
m.show_in_browser()