from collections import defaultdict
from typing import Dict, List

import geopandas
import pandas
import shapely
from geopandas import GeoDataFrame


MAX_LEVEL: int = 5
MIN_LEVEL: int = 0


def read_features() -> geopandas.GeoDataFrame:
    input_shapefile_name = "gadm_410_admin2_complete_geometry.zip"
    features = geopandas.read_file(
        input_shapefile_name,
        engine="pyogrio",
        rows=slice(0, 5000)
    )
    return features


def ignore_field(field: str, target_level: int):
    if field in ("fid_1", "UID"):
        return True
    if str(field).endswith(str(target_level + 1)):
        return True
    return False


def coalesce_sub_features(sub_features: List[GeoDataFrame]) -> GeoDataFrame:
    donor_feature = sub_features[0]
    # print(f"Donor feature: ({type(donor_feature)}", donor_feature)
    new_feature = donor_feature.copy(deep=True)

    donor_admin_level = native_admin_level_of_feature(donor_feature)
    target_admin_level = donor_admin_level - 1

    for field, value in donor_feature.items():
        if ignore_field(field, target_admin_level):
            setattr(new_feature, field, None)
            # new_feature[field] = None
        else:
            setattr(new_feature, field, value)
            # new_feature[field] = value

    new_feature["geometry"] = shapely.union_all([feat.geometry for feat in sub_features])

    # gdf = GeoDataFrame(
    #     new_feature,
    #     crs="EPSG:4326",
    #     index=[0]
    # )

    target_admin_level_str = f"GID_{target_admin_level}"

    print(f"Hey! I made a {type(new_feature)}! It's called {getattr(new_feature, target_admin_level_str).item()}")
    foo = getattr(new_feature, target_admin_level_str)
    print(type(foo))
    print(foo)
    for i in range(0, 6):
        foo = getattr(new_feature, f"GID_{i}").item()
        print(foo)


    return new_feature


def write_to_shapefile(filename: str, final_df: GeoDataFrame):
    # gdf = GeoDataFrame(feature, crs="EPSG:4326", index=[0])
    final_df.to_file(filename, index=False)


def native_admin_level_of_feature(feature: GeoDataFrame) -> int:
    current_level = MAX_LEVEL
    current_level_str = f"GID_{current_level}"

    current_level_id = getattr(feature, current_level_str, None)

    while current_level_id is None and current_level >= MIN_LEVEL:
        current_level -= 1
        current_level_str = f"GID_{current_level}"
        current_level_id = getattr(feature, current_level_str, None)

    if current_level < MIN_LEVEL:
        raise Exception(f"No valid admin level for feature: {feature}")

    return current_level


def insert_feature(feature: GeoDataFrame, feature_hierarchy: List[Dict]):
    # if isinstance(feature, tuple):
    #     feature = GeoDataFrame(feature)

    admin_level = native_admin_level_of_feature(feature)
    admin_level_str = f"GID_{admin_level}"

    print("\t".join((
        # f"UID: {int(feature.UID) if feature.UID is not None else '<None>'}",
        f"Admin Level: {admin_level}",
        f"Name: {getattr(feature, f'NAME_{admin_level}')}"
    )))

    admin_level_id = getattr(feature, admin_level_str).split("_")[0]
    feature_or_uid = feature.UID if feature.UID is not None else feature
    feature_hierarchy[admin_level][admin_level_id]["feature"] = feature_or_uid

    parent_admin_level = admin_level - 1
    while parent_admin_level >= MIN_LEVEL:
        parent_admin_level_str = f"GID_{parent_admin_level}"
        parent_admin_level_id = getattr(feature, parent_admin_level_str).split("_")[0]
        feature_hierarchy[parent_admin_level][parent_admin_level_id]["sub_features"].append(admin_level_id)
        parent_admin_level -= 1


def get_feature(identifier: str, all_the_features: List[Dict], file_features):
    native_admin_level = identifier.count(".")
    stripped_identifier = identifier.split("_")[0]

    feature_info = all_the_features[native_admin_level][stripped_identifier]

    feature_or_uid = feature_info["feature"]

    if isinstance(feature_or_uid, float):
        print(f"Found UID {feature_or_uid} for feature {stripped_identifier}")
        feature = file_features.loc[file_features["UID"] == feature_or_uid]
    elif isinstance(feature_or_uid, GeoDataFrame):
        print(f"Found Dataframe for feature {stripped_identifier}")
        feature = feature_or_uid
    else:
        print(f"Found {type(feature_or_uid)} for feature {stripped_identifier}, recursing...")
        if not feature_info["sub_features"]:
            print(f"WARNING: No sub-features found for {stripped_identifier}")
            return None

        sub_feature_ids = feature_info["sub_features"]
        sub_features = [
            get_feature(sub_feature, all_the_features, file_features)
            for sub_feature in sub_feature_ids
        ]
        feature = coalesce_sub_features([
            sub_feature for sub_feature in sub_features
            if sub_feature is not None
        ])
        insert_feature(feature, all_the_features)
    return feature


if __name__ == "__main__":
    # First, read the source file and register each feature in a hierarchy
    # both at its native level and as a sub-feature of each feature above it
    feature_hierarchy = [
        defaultdict(lambda: {"sub_features": list(), "feature": None})
        for level in range(MIN_LEVEL, MAX_LEVEL + 1)
    ]
    file_features = read_features()
    for feature in file_features.itertuples():
        insert_feature(feature, feature_hierarchy)

    features_to_write = list()

    # Hardcode extracting/constructing Illinois for the moment to allow
    # for faster debugging
    identifier = "USA.14"

    features_to_write.append(
        get_feature(identifier, feature_hierarchy, file_features)
    )

    concatted_dfs = pandas.concat(features_to_write, ignore_index=True, axis=0)
    write_to_shapefile("maybe_illinois.shp", concatted_dfs)
