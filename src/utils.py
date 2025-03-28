import os
import pandas as pd
from autogluon.timeseries import TimeSeriesDataFrame

def process_cluster_data(input_dir, output_dir, cluster_prefix="cluster_"):
    """
    Processes cluster data by reading and combining all parquet files within each cluster folder,
    converting the combined DataFrame into a TimeSeriesDataFrame, and saving the result.

    Parameters:
        input_dir (str): Path to the directory containing cluster folders.
        output_dir (str): Path where the combined parquet files will be saved.
        cluster_prefix (str): Prefix to identify cluster folders (default is "cluster_").

    Raises:
        ValueError: If the combined DataFrame does not have a MultiIndex.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Sort cluster folders numerically based on the number following the prefix
    cluster_dirs = sorted(
        [d for d in os.listdir(input_dir) if d.startswith(cluster_prefix)],
        key=lambda x: int(x.split('_')[1])
    )

    for cluster_name in cluster_dirs:
        cluster_path = os.path.join(input_dir, cluster_name)
        print(f"Processing: {cluster_path}")

        if os.path.isdir(cluster_path):
            dfs = []

            # Read all parquet files in the current cluster folder
            for file in os.listdir(cluster_path):
                if file.endswith('.parquet'):
                    file_path = os.path.join(cluster_path, file)
                    df = pd.read_parquet(file_path)
                    dfs.append(df)

            if dfs:
                df_combined = pd.concat(dfs, ignore_index=False)

                if not isinstance(df_combined.index, pd.MultiIndex):
                    raise ValueError(f"{cluster_name}: Combined DataFrame does not have a MultiIndex.")

                # Convert the combined DataFrame to a TimeSeriesDataFrame and reset the index
                chronos_final = TimeSeriesDataFrame(df_combined)
                chronos_final = chronos_final.reset_index()

                # Save the resulting DataFrame to the output directory
                output_path = os.path.join(output_dir, f"{cluster_name}_combined.parquet")
                chronos_final.to_parquet(output_path, index=False)
                print(f"Saved: {output_path}")

def process_all_clusters(base_input_dir, base_output_dir, types=['dtw', 'embeddings', 'statistical'], cluster_prefix="cluster_"):
    """
    Processes clusters for all specified types by looping through each type folder,
    reading the clusters, and saving the combined TimeSeriesDataFrame for each.

    Parameters:
        base_input_dir (str): Base directory containing type folders.
        base_output_dir (str): Base directory where output type folders will be saved.
        types (list): List of type folder names (default is ['dtw', 'embeddings', 'statistical']).
        cluster_prefix (str): Prefix to identify cluster folders (default is "cluster_").
    """
    for t in types:
        input_dir = os.path.join(base_input_dir, t)
        output_dir = os.path.join(base_output_dir, t)
        print(f"\n--- Processing type: {t} ---")
        process_cluster_data(input_dir, output_dir, cluster_prefix)
