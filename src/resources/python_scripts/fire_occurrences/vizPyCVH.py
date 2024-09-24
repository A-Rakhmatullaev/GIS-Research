import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os
import folium
import glob
import io
from PIL import Image
from folium.plugins import MousePosition


dir_path = os.path.dirname(os.path.abspath(__file__))
all_points_files = glob.glob(os.path.join(dir_path, '..', '..', 'fire_occurrences', 'output', 'convex_hull', 'convex_result.json', 'part-*'))

# Read the GeoJSON file
try:
    # gdf = gpd.read_file(file_path)

    # # # Plot the data
    # # gdf.plot()
    # # plt.show()
    # # Create a Folium map centered around the world
    m = folium.Map(location=[0, 0], zoom_start=2)

    # Add GeoJSON layer to the map
    #folium.GeoJson(gdf).add_to(m)

    points_gdf = gpd.GeoDataFrame()

    for file in all_points_files:
        gdf = gpd.read_file(file)
        points_gdf = pd.concat([points_gdf, gdf], ignore_index=True)

    # Add all the boundary points to the map
    folium.GeoJson(points_gdf, name="Points").add_to(m)

    # Display the map
    #m.save("NuclearPSConvexHullMap.html")  # Save the map as an HTML file
    formatter = "function(num) {return L.Util.formatNum(num, 3) + ' º ';};"
    MousePosition(
        position="topright",
        separator=" | ",
        empty_string="NaN",
        lng_first=False,
        num_digits=20,
        prefix="Coordinates:",
        lat_formatter=formatter,
        lng_formatter=formatter,
    ).add_to(m)
    m.show_in_browser()

    # img_data = m._to_png(5)
    # img = Image.open(io.BytesIO(img_data))
    # img.save('image.png')

except Exception as e:
    print(f"Error reading GeoJSON file: {e}")