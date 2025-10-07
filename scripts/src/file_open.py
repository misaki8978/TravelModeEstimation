#複数ファイル -- 単一ファイル
def division_several_file(files):
    split_idx = files.index('--')
    # log_message(f"{split_idx}", log_path)
    sevral_files, one_file = files[:split_idx], files[split_idx + 1]
    # log_message(f"{path_parts}", log_path)
    return sevral_files, one_file

#単一ファイル -- 単一ファイル
def division_two_file(files):
    split_idx = files.index('--')
    # log_message(f"{split_idx}", log_path)
    first_file, second_file = files[split_idx -1], files[split_idx + 1]
    # log_message(f"{path_parts}", log_path)
    return first_file, second_file


#geopandas 複数ファイル -- 単一ファイル
def division_several_file(files):
    split_idx = files.index('--')
    # log_message(f"{split_idx}", log_path)
    sevral_files, gis_file = files[:split_idx], files[split_idx + 1]
    gis_df = gpd.read_file(gis_file)
    # bus_routes = bus_df[bus_df.geometry.geom_type.isin(["LineString", "MultiLineString"])]
    gis_gdf = gis_df.to_crs(epsg=32652)
    return  sevral_files, gis_gdf