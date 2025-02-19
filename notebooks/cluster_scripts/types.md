## Types of clustering methods:
#### 1. Feature-Based Clustering:
Extract statistical (mean, standard deviation, skewness, kurtosis) and other descriptive features from each time series, then use standard algorithms like K‑Means or hierarchical clustering on the feature vectors.

#### 2. Dynamic Time Warping (DTW) Clustering:
Compute pairwise DTW distances to capture similarity even with time shifts. Cluster the resulting distance matrix using methods such as hierarchical clustering or DBSCAN.

#### 3. Autoencoder-Based Clustering:
LSTM Autoencoder: Learns compressed features → use K-Means or GMM on encoded representation.

#### 4. Embedding-Based Clustering:
Learn representations (embeddings) with deep models such as autoencoders or recurrent neural networks. Then, cluster the embeddings (e.g., with K‑Means) to group similar dynamics.

#### 5. Model-Based Clustering:
Fit time series models (e.g., ARIMA or exponential smoothing) to each series and cluster based on the estimated model parameters.

#### 6. Frequency Domain Clustering: 
Fourier Transform, Wavelet Transform