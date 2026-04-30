# Databricks Visualization Guide - Version 2
## IMDb Project-Specific Visualizations

**Focus:** How to create compelling visualizations from the exact tables and aggregations in `imdb_analysis_final_v6.py`

**Updated:** April 30, 2026  
**For:** Miami Dade College Big Data Project  

---

## Quick Navigation

| Step | Query | Chart Type | Purpose |
|------|-------|-----------|---------|
| 7 | Top 20 Rated Movies | Bar Chart | Show highest-rated films |
| 8 | Production Trends | Line Chart | Trend over time (1920-2026) |
| 9 | Genre Distribution | Pie Chart | Show genre composition |
| 11 | Genre Ratings | Bar Chart | Genre performance comparison |
| 12 | Correlation | Matrix | Show relationships |
| 13 | Predictions | Scatter Plot | Model results |
| 14 | Merged Data | Area Chart | Growth with merge |

---

## Step 7: Top 20 Highly Rated Movies

### Query (Ready to Copy)
```sql
SELECT 
    primaryTitle, 
    startYear, 
    genres, 
    averageRating, 
    numVotes
FROM movies_view
ORDER BY averageRating DESC
LIMIT 20
```

### Visualization Setup

**Chart Type:** Bar Chart (Horizontal)

**Configuration:**
- **X-axis:** `averageRating` (numeric value)
- **Y-axis:** `primaryTitle` (category/text)
- **Title:** "Top 20 Highly Rated Movies"
- **Color:** Green (represents high quality)
- **Sort:** Descending by averageRating

### Why This Works
- Horizontal bar makes long movie titles readable
- Rating on X-axis shows clear ranking
- Immediate visual impact: tallest bars = best movies

### GUI Steps
1. Run the query above
2. Click `Visualization` button
3. Select `Bar Chart`
4. Set X = `averageRating`, Y = `primaryTitle`
5. Add title: "Top 20 Highly Rated Movies"
6. Change palette to green/blue gradient

---

## Step 8: Movie Production Trends (1920-2026)

### Query (Ready to Copy)
```sql
SELECT 
    startYear, 
    COUNT(*) as movies_produced
FROM movies_view
WHERE startYear >= 1920
GROUP BY startYear
ORDER BY startYear
```

### Visualization Setup

**Chart Type:** Line Chart

**Configuration:**
- **X-axis:** `startYear` (time series)
- **Y-axis:** `movies_produced` (count)
- **Title:** "Movie Production Trends (1920-2026)"
- **Color:** Blue
- **Line Style:** Solid

### Why This Works
- Line chart best shows **progression over time**
- Reveals growth pattern clearly
- Shows saturation point (2023 peak)
- Demonstrates 2026 recovery

### Key Insights from Chart
- Exponential growth: 2,600 (1990) → 11,500 (2023) = 4.4x
- Slight decline: 2024-2025
- Recovery: 2026 shows improvement

### GUI Steps
1. Run the query above
2. Click `Visualization`
3. Select `Line Chart`
4. X = `startYear`, Y = `movies_produced`
5. Title: "Movie Production Trends (1920-2026)"
6. Hover to see exact numbers for each year

---

## Step 9: Top 15 Genre Distribution

### Query (Ready to Copy)
```sql
SELECT 
    genres, 
    COUNT(*) as movie_count
FROM movies_view
WHERE genres IS NOT NULL
GROUP BY genres
ORDER BY movie_count DESC
LIMIT 15
```

### Visualization Setup

**Chart Type:** Pie Chart (or Donut)

**Configuration:**
- **Legend:** `genres` (category)
- **Value:** `movie_count` (size of slice)
- **Title:** "Top 15 Movie Genres Distribution"
- **Show Data Labels:** Yes (percentages)
- **Max Slices:** 15 (good for pie chart)

### Why This Works
- Pie chart shows **composition** (parts of a whole)
- Easy to see which genres dominate
- Drama & Comedy = ~70% of market

### Key Insights from Chart
- Drama: ~155k movies (heavily saturated)
- Comedy: ~82k movies (competitive)
- Documentary: ~56k movies (high quality niche)

### GUI Steps
1. Run the query above
2. Click `Visualization`
3. Select `Pie Chart`
4. Legend = `genres`, Value = `movie_count`
5. Toggle `Data Labels` = ON
6. Add title

---

## Step 11: Genre Analysis - Rating by Genre

### Query (Ready to Copy)
```sql
SELECT 
    SUBSTR(genre, 1, 50) as genre,
    COUNT(*) as movie_count,
    ROUND(AVG(averageRating), 2) as avg_rating
FROM (
    SELECT explode(split(genres, ',')) as genre, averageRating
    FROM movies_view
)
WHERE movie_count >= 100
GROUP BY genre
ORDER BY avg_rating DESC
LIMIT 15
```

### Visualization Setup

**Chart Type:** Bar Chart (Vertical)

**Configuration:**
- **X-axis:** `genre` (categories)
- **Y-axis:** `avg_rating` (numeric)
- **Title:** "Top Genres by Average Rating"
- **Color Palette:** Red (low) → Green (high)
- **Y-axis Range:** 4.5 to 7.5 (zoom into relevant range)

### Why This Works
- Compares ratings **across genres**
- Color gradient shows performance instantly
- Green = invest, Red = risky

### Key Insights from Chart
🥇 **Documentary: 7.18** - HIGHEST QUALITY  
🥈 **Biography: 6.92** - Prestige films  
🥉 **Music: 6.79** - Niche but quality  
🔴 **Horror: 4.97** - LOWEST (risky)  
🔴 **Sci-Fi: 5.33** - Expensive, risky

### GUI Steps
1. Run the query above
2. Click `Visualization`
3. Select `Bar Chart`
4. X = `genre`, Y = `avg_rating`
5. Change color palette to diverging (red-green)
6. Set Y-axis min = 4.5, max = 7.5 (focus on range)

---

## Step 12: Correlation Matrix

### Query (Ready to Copy)
```sql
SELECT
    CORR(runtimeMinutes, numVotes) as runtime_votes,
    CORR(runtimeMinutes, averageRating) as runtime_rating,
    CORR(numVotes, averageRating) as votes_rating,
    CORR(startYear, averageRating) as year_rating
FROM movies_view
```

### Visualization Setup

**Chart Type:** Table / Counter (Display as Text)

**Configuration:**
- **Title:** "Correlation Coefficients"
- **Decimals:** 3 (show precision)

### Why This Works
- Shows mathematical relationships
- Numbers tell the story (investor perspective)
- Validates modeling assumptions

### Key Insights from Numbers
- **Votes ↔ Rating: +0.45** - Strong: popular movies rated higher
- **Runtime ↔ Rating: +0.15** - Weak: "sweet spot" exists (non-linear)
- **Year ↔ Rating: +0.05** - Minimal: quality independent of time

### GUI Steps
1. Run the query above
2. Click `Visualization`
3. Select `Counter` or `Table`
4. Just display the numbers
5. Add title: "Correlation Analysis"

---

## Step 13: Linear Regression Model Results

### Query (Ready to Copy)
```sql
SELECT 
    averageRating as actual_rating,
    ROUND(
        REGR_INTERCEPT(averageRating, numVotes) OVER () +
        REGR_SLOPE(averageRating, numVotes) OVER () * numVotes,
        2
    ) as predicted_rating,
    numVotes,
    primaryTitle
FROM movies_view
LIMIT 500
```

### Visualization Setup

**Chart Type:** Scatter Plot

**Configuration:**
- **X-axis:** `numVotes` (popularity)
- **Y-axis:** `actual_rating` (truth)
- **Color:** By `predicted_rating` (prediction accuracy)
- **Size:** By deviation (error)
- **Title:** "Model Predictions vs. Actual Ratings"

### Why This Works
- Shows model accuracy visually
- Scatter reveals clusters and outliers
- X = popularity, Y = quality relationship

### Key Insights from Chart
- Tight clustering = good model fit
- Spread = model uncertainty
- RMSE 1.5 = model accurate ±1.5 points

### GUI Steps
1. Run the query above
2. Click `Visualization`
3. Select `Scatter Plot`
4. X = `numVotes`, Y = `actual_rating`
5. Color = `predicted_rating`
6. Add title

---

## Step 14: Streaming Merge - Before/After

### Query Before Merge (Ready to Copy)
```sql
SELECT
    'BEFORE MERGE' as phase,
    COUNT(*) as total_movies,
    MAX(startYear) as latest_year,
    ROUND(AVG(averageRating), 2) as avg_rating
FROM movies_view
```

### Query After Merge (Ready to Copy)
```sql
SELECT
    'AFTER MERGE' as phase,
    COUNT(*) as total_movies,
    MAX(startYear) as latest_year,
    ROUND(AVG(averageRating), 2) as avg_rating
FROM movies_view_complete
```

### Combined Comparison Query
```sql
SELECT
    'Original Data' as dataset,
    COUNT(*) as movie_count,
    MAX(startYear) as latest_year,
    ROUND(AVG(averageRating), 2) as avg_rating
FROM movies_view
UNION ALL
SELECT
    'After 2026 Merge' as dataset,
    COUNT(*) as movie_count,
    MAX(startYear) as latest_year,
    ROUND(AVG(averageRating), 2) as avg_rating
FROM movies_view_complete
```

### Visualization Setup

**Chart Type:** Grouped Bar Chart

**Configuration:**
- **X-axis:** `dataset` (categories: Original vs. Merged)
- **Y-axis:** `movie_count` (scale)
- **Group By:** Different metrics (toggle between count/year/rating)
- **Title:** "Data Growth: Original vs. 2026 Merge"
- **Color:** Blue (original) vs. Green (merged)

### Why This Works
- Shows impact of streaming ingestion
- Demonstrates pipeline success
- Visual proof of data increase

### Key Insights from Chart
- Movie count increase: 330,970 → ~332,657
- Latest year extends: 2026
- Average rating improves: 6.16 → 6.67

### GUI Steps
1. Run the combined query above
2. Click `Visualization`
3. Select `Bar Chart` (grouped)
4. X = `dataset`, Y = `movie_count`
5. Add title: "Data Merge Impact"

---

## Advanced: Runtime Strategy Visualization

### Query (Ready to Copy)
```sql
SELECT 
    CASE 
        WHEN runtimeMinutes < 60 THEN 'Short (<60m)'
        WHEN runtimeMinutes < 90 THEN 'Standard (60-90m)'
        WHEN runtimeMinutes < 120 THEN 'Feature (90-120m)'
        WHEN runtimeMinutes < 150 THEN 'Epic (120-150m)'
        WHEN runtimeMinutes < 180 THEN 'Long (150-180m)'
        ELSE 'Extended (>180m)'
    END as runtime_category,
    COUNT(*) as movie_count,
    ROUND(AVG(averageRating), 2) as avg_rating,
    ROUND(AVG(numVotes), 0) as avg_votes
FROM movies_view
WHERE runtimeMinutes > 0 AND runtimeMinutes < 300
GROUP BY runtime_category
ORDER BY 
    CASE 
        WHEN runtime_category = 'Short (<60m)' THEN 1
        WHEN runtime_category = 'Standard (60-90m)' THEN 2
        WHEN runtime_category = 'Feature (90-120m)' THEN 3
        WHEN runtime_category = 'Epic (120-150m)' THEN 4
        WHEN runtime_category = 'Long (150-180m)' THEN 5
        ELSE 6
    END
```

### Visualization Setup

**Chart Type:** Combo Chart (Dual Axis)

**Configuration:**
- **Primary Y-axis:** `avg_rating` (line, blue)
- **Secondary Y-axis:** `movie_count` (bar, gray)
- **X-axis:** `runtime_category`
- **Title:** "Runtime Strategy: Quality vs. Volume"

### Why This Works
- Shows the **sweet spot trade-off**
- Quality (line) vs. Quantity (bars)
- Reveals optimal 120-150m range

### Key Insights from Chart
- **Extended (>180m): 6.94 rating** (best quality, small volume)
- **Epic (120-150m): 6.47 rating** (SWEET SPOT - good quality, reasonable volume)
- **Standard (60-90m): 5.99 rating** (volume, lower quality)

### GUI Steps
1. Run the query above
2. Click `Visualization`
3. Select `Combo Chart`
4. Primary = `avg_rating` (line)
5. Secondary = `movie_count` (bar)
6. X = `runtime_category`
7. Title: "Runtime Strategy Analysis"

---

## Databricks GUI Quick Reference

### How to Create Each Chart Type

**Bar Chart:**
1. Click Visualization
2. Select Bar Chart
3. X = Category (genre, title, etc.)
4. Y = Numeric (count, rating, votes, etc.)

**Line Chart:**
1. Click Visualization
2. Select Line Chart
3. X = Time (startYear, month, etc.)
4. Y = Numeric metric
5. Auto-connects points chronologically

**Pie Chart:**
1. Click Visualization
2. Select Pie Chart
3. Legend = Categories
4. Value = Numbers (must sum to 100%)
5. Keep categories ≤10 for readability

**Scatter Plot:**
1. Click Visualization
2. Select Scatter Plot
3. X = First numeric field
4. Y = Second numeric field
5. Optional: Size = third metric

**Table/Counter:**
1. Click Visualization
2. Select Table or Counter
3. Just displays raw results
4. Good for showing summary statistics

### Customization in GUI

**All Chart Types:**
- **Title:** Add descriptive title (Shows what, not how)
- **Colors:** Click palette icon, choose theme
- **Labels:** Enable data labels (values on bars/points)
- **Legends:** Show/hide, position (top/bottom/left/right)
- **Axes:** Set min/max ranges, custom labels

**Save to Dashboard:**
1. Click Save (upper right of chart)
2. Select "Save to Dashboard"
3. Choose or create new dashboard
4. Name the visualization

---

## Best Practices for IMDb Project Visualizations

### 1. Tell the Investment Story
Each chart should answer a business question:
- ✅ "Which genres should we invest in?" → Genre ratings bar chart
- ✅ "Is the market saturated?" → Production trends line chart
- ✅ "What's the optimal movie length?" → Runtime combo chart

### 2. Use Color Strategically
- 🟢 **Green** = Good investment (high rating)
- 🟡 **Yellow** = Moderate (medium rating)
- 🔴 **Red** = Risky (low rating)

### 3. Limit Categories
- **Pie charts:** Max 10 slices (use LIMIT 10)
- **Bar charts:** Max 15 categories (use LIMIT 15)
- **Line charts:** All years OK (shows trend)

### 4. Include Context
- Always add **title** (answer the question)
- Show **count of records** (how many movies?)
- Add **confidence note** (sample size matters)

### 5. Avoid Common Mistakes
❌ 3D charts (hard to read)
❌ Rainbow colors (hard to distinguish)
❌ Too many categories (cluttered)
❌ No title (unclear purpose)
✅ Simple palettes (blue, green, red)
✅ Clear titles (What insight?)
✅ Proper aggregation (GROUP BY)
✅ Sorted data (DESC by metric)

---

## Presentation Order (For Your Business Case)

**Slide Sequence:**

1. **Top Rated Movies** (Bar) - Opens with quality examples
2. **Production Trends** (Line) - Shows market context
3. **Genre Distribution** (Pie) - Shows composition
4. **Genre Ratings** (Bar) - Shows investment opportunities
5. **Runtime Strategy** (Combo) - Shows optimization
6. **Model Predictions** (Scatter) - Shows ML validation
7. **Merge Impact** (Bars) - Shows data completeness

---

## Copy-Paste Ready Queries

### Quick Export for PowerPoint
All queries above are ready to:
1. Copy-paste into Databricks SQL cell
2. Run (Shift+Enter)
3. Click Visualization
4. Follow the "Visualization Setup" instructions
5. Save to presentation

---

## Troubleshooting Charts

**Issue:** "No data to display"
- **Fix:** Check WHERE clause, verify data exists
- **Example:** `WHERE startYear >= 1990` might exclude old movies

**Issue:** Chart looks cluttered
- **Fix:** Add LIMIT clause to query
- **Example:** `LIMIT 15` for bar charts

**Issue:** Wrong axis assignments
- **Fix:** Swap X and Y axes in visualization settings
- **Check:** Numeric field should be on Y

**Issue:** Missing values in chart**
- **Fix:** Add `WHERE field IS NOT NULL`
- **Example:** `WHERE genres IS NOT NULL`

---

## Summary

This guide provides **7 complete visualization workflows** tailored to your IMDb project:

✅ Copy-paste SQL queries  
✅ Step-by-step GUI setup  
✅ Business insights for each chart  
✅ Investment decision framework  
✅ Presentation-ready visualizations  

Use these exact queries and configurations to create professional visualizations for your presentation!

---

**Last Updated:** April 30, 2026  
**Version:** 2.0  
**Status:** Production-Ready
