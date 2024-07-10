import geopandas as gpd
import matplotlib.pyplot as plt
import os
import folium


dir_path = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(dir_path, 'nuclear', 'output', 'result.json', 'part-00000')

print(file_path)

# Read the GeoJSON file
try:
    gdf = gpd.read_file(file_path)

    # # Plot the data
    # gdf.plot()
    # plt.show()
    # Create a Folium map centered around the world
    m = folium.Map(location=[0, 0], zoom_start=2)

    # Add GeoJSON layer to the map
    folium.GeoJson(gdf).add_to(m)

    # Display the map
    m.save("NuclearPSConvexHullMap.html")  # Save the map as an HTML file
    m.show_in_browser()

except Exception as e:
    print(f"Error reading GeoJSON file: {e}")