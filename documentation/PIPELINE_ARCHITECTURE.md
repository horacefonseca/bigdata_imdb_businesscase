# IMDb Project Pipeline Architecture

This diagram illustrates the end-to-end data flow of the IMDb Big Data Analysis project, from raw fragmented ingestion to final predictive analytics.

```mermaid
sequenceDiagram
    participant Volume as Unity Catalog Volumes
    participant ETL as PySpark ETL Pipeline
    participant Stats as Statistical Engine
    participant ML as MLlib Pipeline
    participant Dashboard as Databricks Dashboard

    Note over Volume, ETL: Data Ingestion Phase
    Volume->>ETL: Load title_basics.tsv (Metadata)
    Volume->>ETL: Load title_ratings.tsv (Ratings)
    
    Note over ETL: Cleaning & Integration
    ETL->>ETL: Replace \N with NULL
    ETL->>ETL: Inner Join on tconst
    ETL->>ETL: Filter titleType == 'movie'
    ETL->>ETL: Cast numeric types (Year, Runtime, Rating)

    Note over Stats: Advanced Analytics (Mod 4-6)
    ETL->>Stats: Explode Genres (Normalization)
    ETL->>Stats: Generate Correlation Matrix
    Stats->>Dashboard: Genre Trends & Yearly Averages

    Note over ML: Predictive Modeling (Mod 7)
    ETL->>ML: VectorAssembler (Feature Engineering)
    ML->>ML: LinearRegression (Predict averageRating)
    ML->>Dashboard: Prediction Results & RMSE Evaluation

    Note over ETL, Volume: Streaming Simulation (Mod 5)
    Volume-->>ETL: readStream new_movies_2026.tsv
    Volume-->>ETL: readStream new_ratings_2026.tsv
    ETL-->>Dashboard: Live 2026 Most Anticipated Update
```

## Stage Descriptions

1.  **Data Ingestion**: Multi-source ingestion from Databricks Unity Catalog Volumes.
2.  **Cleaning & Integration**: Standards-compliant handling of IMDb NULL strings (`\N`) and relational joining.
3.  **Genre Explosion**: Advanced transformation to handle multi-genre strings for granular category analysis.
4.  **Statistical Correlation**: Mathematical validation of relationships between movie runtime, popularity, and ratings.
5.  **Machine Learning**: A supervised learning pipeline using Spark MLlib to predict movie ratings based on historical metadata.
6.  **Simulated Streaming**: Real-time ingestion simulation for "2026 Updates" using structured streaming logic.
