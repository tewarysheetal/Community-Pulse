# Community Pulse

Champaign, Illinois has a measurement problem.

Standard poverty metrics systematically undercount economic hardship in university cities. A large transient student population suppresses median income, inflates apparent low-income household counts, and makes year-over-year trend analysis unreliable. Community organizations making funding and policy decisions are working off numbers that don't reflect the people they serve.

This project rebuilds that picture from the ground up.

---

## The Question

**How many working-family households in Champaign fall below a true economic survival threshold — and how has that changed across 2019 to 2023?**

The federal poverty line isn't designed to answer this. It misses the broader population of households that earn above poverty but still cannot cover basic necessities. The **ALICE framework** (Asset Limited, Income Constrained, Employed) captures this gap by defining survival budgets by household composition and local cost of living — a far more precise instrument for community-level analysis.

---

## The Approach

Three Census data products and a layered analytical pipeline feed the final outputs:

**ACS 5-Year Estimates** establish the demographic and economic baseline — income distribution, housing cost burden, employment, language, and household composition across ten subject areas from 2019 to 2023.

**PUMS Microdata** provides the household-level records needed for ALICE analysis. Each record carries Census survey weights, allowing population-representative estimates from a sample. The pipeline uses this to build household income profiles, classify composition, inflation-adjust income across years, and apply ALICE thresholds at the household level.

**ACS Analysis Notebooks** form a downstream analytical layer built on top of the ACS fact tables. They cover exploratory data analysis, tract geography enrichment, K-Means clustering, cluster-to-cluster transitions across years, and a hardship-risk bridge that allocates known county-level ALICE household totals down to individual census tracts.

Four analytical challenges required custom engineering:

**Student population isolation** — Champaign's student households are flagged using a combination of school enrollment status and employment absence across household members. The ALICE analysis runs in parallel on the full population and the non-student subset, and both are exported for comparison.

**PUMA geography correction (2022–2023)** — Census PUMA boundaries were redrawn after 2021, splitting Champaign County across two reporting areas. One of those areas extends into neighboring counties, making a simple geographic filter incorrect. The pipeline constructs a tract-level allocation factor (alpha) derived from ACS5 household counts to proportionally weight records back to Champaign County only.

**Threshold calibration** — ALICE survival budgets for prior years are backfilled using the Illinois Essentials Index. The two dominant childless household types receive an additional local calibration adjustment before any below-ALICE flags are set. A validation suite checks threshold consistency, null integrity, population balance, and benchmark comparisons before outputs are finalized.

**Tract-level ALICE estimation** — County-level ALICE household counts derived from PUMS are disaggregated to individual census tracts using a composite ACS hardship risk score. Each tract receives an allocation weight proportional to its risk score, with an optional student-population dampening factor. Tract estimates are constrained to sum exactly to the known county total for each year.

---

## Core Concepts & Techniques

### Data Ingestion & Loading

**Live Census API ingestion** — ACS5 tract-level household counts are pulled directly from the Census Bureau REST API at runtime, parameterized by county and year, and loaded straight into PostgreSQL. No manual downloads for reference data.

**Automated file inventory** — Before any ACS data is processed, a regex-based file scanner walks the raw download directory, classifies every file by table code, source type (Detail/Subject/Profile), and year, and produces an inventory with a missing-file report. The pipeline won't proceed against an incomplete dataset.

**PostgreSQL COPY bulk loading** — PUMS files contain millions of records. Rather than row-by-row inserts, the pipeline streams CSV data through `COPY ... FROM STDIN`, cutting load times dramatically and keeping memory overhead flat regardless of file size.

---

### Data Modeling

**Star schema** — Both pipelines share a consistent modeling philosophy. Dimension tables hold stable reference data — `dim_tract` for Census geography, `alice_thresholds` for ALICE survival budgets, `alice_puma1902_alpha` for geographic allocation factors, and tract-to-PUMA crosswalks. Fact tables hold the analytical records. Queries join against dimensions rather than embedding logic in every table, making threshold changes and recalibrations a single-point update.

**Multi-layer ETL (Staging → Intermediate → Fact)** — The ACS pipeline processes data through three explicit layers. Raw CSV files are loaded into staging tables exactly as downloaded. Intermediate views apply column selection, renaming, and thematic grouping across related ACS tables (housing, poverty, demographics). The final fact tables are built from those intermediate views, fully cleaned and analytics-ready. Each layer is independently validated before the next one runs.

**Wide-to-long pivoting** — ACS tables are delivered in wide format with hundreds of metric columns per row. The fact layer unpivots this into a long format — one row per tract × year × metric — using `pandas.melt()`. This makes cross-table queries, trend analysis, and Tableau connections dramatically simpler, at the cost of a larger row count.

---

### Analytical Methods

**Inflation-adjusted income normalization** — PUMS includes a Census-provided adjustment factor (`adjinc`) per record that normalizes nominal income to a consistent reference year. Applied as `hincp × adjinc / 1,000,000`, this makes household income directly comparable across all five survey years without external CPI lookups.

**AEI ratio-based threshold backfilling** — The 2023 ALICE survival budgets are the ground truth. Prior years are derived by scaling those budgets by the ratio of each year's Illinois Essentials Index to the 2023 index value — a principled backfill that preserves the structural relationship between years without requiring independent budget reconstruction for each one.

**Fractional geographic allocation** — When a PUMA spans multiple counties, a simple boundary filter misattributes households. The pipeline builds a per-PUMA allocation factor (alpha) — the share of total PUMA households belonging to Champaign County — derived from ACS5 tract counts and applied as a survey weight multiplier. This preserves population estimates while correcting for geographic spillover.

**Survey-weighted estimation** — Raw PUMS counts are meaningless without weights. Every household count, income average, and composition breakdown is computed using the Census-assigned household weight (`wgtp`), producing population-representative estimates from a sample rather than literal row counts.

**Margin of error tracking** — ACS data ships with a margin of error (MOE) alongside every point estimate. The pipeline preserves both, tagging each metric as `estimate`, `moe`, or `derived` so downstream analysis can surface statistical reliability alongside the numbers themselves.

**K-Means clustering with cross-year alignment** — ACS tract profiles are clustered independently for each survey year using K-Means on a curated set of 21 economic and demographic indicators. Because cluster labels are arbitrary across independent runs, a centroid-distance matching step (Hungarian algorithm via `scipy.optimize.linear_sum_assignment`) aligns cluster identities across years so that `aligned_cluster_0` refers to the same community type in 2019 and 2023. Elbow and silhouette diagnostics guide K selection. PCA projections are exported for visual validation.

**Stable-tract transition analysis** — Census tracts that appear in all four survey years are isolated and their aligned cluster assignments are tracked year-over-year. Transition matrices, Sankey-ready edge tables, and wide-format path tables expose whether tracts have shifted community type over the 2019–2023 period.

---

### Engineering Practices

**Programmatic SQL generation** — Rather than maintaining near-identical SQL scripts for five survey years, both pipelines generate them at runtime from Python templates. Year-specific parameters — table names, PUMA filters, weight columns — are injected via f-strings. The resulting SQL is written to disk for auditability and executed immediately against the database.

**GEO ID parsing** — Census full GEO IDs (e.g., `1400000US17019XXXXXX`) are decoded at ingest to extract state FIPS, county FIPS, and tract codes as structured columns. This makes geographic joins and filters consistent across every table, regardless of source format.

**Population balance validation** — Every pipeline run verifies that `below_alice + above_alice = total_households` across all years before outputs are trusted. Combined with null checks, threshold consistency checks, and benchmark comparisons against published ALICE data sheets, this ensures the pipeline fails loudly rather than silently producing incorrect counts.

---

## Outputs

**PUMS / ALICE pipeline** — Tableau-ready statistical profiles of the ALICE population by year: household counts, income distribution, composition breakdowns, and threshold proximity. Includes a nonstudent vs. full-population comparison designed for direct use by community stakeholders.

**ACS analysis notebooks** — Tract-level outputs covering EDA summaries and visualisations, a client-friendly geography lookup (centroid coordinates, place/ZIP overlays, area-type labels), K-Means cluster assignments and centroid profiles, cross-year cluster transition matrices, and a final tract-level ALICE household estimate table that allocates county ALICE totals proportionally across tracts.

---

## Stack

Python · PostgreSQL · pandas · SQLAlchemy · Census API · JupyterLab · Tableau · geopandas · scikit-learn · scipy

---

## Replication

Full setup, pipeline execution order, and schema reference: **[docs/REPLICATION.md](docs/REPLICATION.md)**
