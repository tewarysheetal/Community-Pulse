# Community Pulse

> How many working families in Champaign, Illinois can't make ends meet — and are we even counting the right people?

Standard poverty measures say one thing. The reality on the ground says another. In a city shaped by a major university, a large student population quietly distorts every income and household statistic, making economic hardship look smaller than it actually is.

Community Pulse exists to fix that.

---

## The Problem

Federal poverty measures capture only the most extreme hardship. They miss the much larger group of households that earn above the poverty line but still cannot afford basic necessities — rent, food, childcare, healthcare, transportation. This population is known as **ALICE: Asset Limited, Income Constrained, Employed**.

In Champaign, measuring ALICE accurately has an added complication: tens of thousands of college students live here. They show up in Census data as low-income households, but they are not the economically vulnerable families that community organizations need to serve. Counting them inflates hardship numbers and muddies every trend line.

**The question this project answers:** Among Champaign's actual working-family population, how many households fall below the ALICE survival threshold — and how has that changed from 2019 to 2023?

---

## How We Solve It

Two Census data sources, one integrated pipeline:

**ACS Summary Tables** give us the broad demographic and economic picture of Champaign — income distribution, housing costs, employment, and more — across ten subject areas from 2019 to 2023.

**PUMS Microdata** gives us individual household records. We use this to build household-level profiles, calculate inflation-adjusted income, classify each household by composition (adults + children), and compare their income against the ALICE survival budget for that household type.

The key engineering decisions that make the analysis trustworthy:

- **Student filter** — households where the majority of members are enrolled students with no employment are flagged and separated, so the ALICE count reflects working families, not students.
- **PUMA boundary correction** — Census geography changed in 2022, splitting Champaign County across two reporting areas. We build a tract-level allocation factor from ACS5 household counts to proportionally assign the overlapping geography back to Champaign only.
- **Calibrated thresholds** — ALICE survival budgets are adjusted upward for the two most common childless household types to reflect Champaign's actual cost of living.
- **Validation suite** — every pipeline run checks threshold accuracy, null integrity, population balance, and benchmark comparisons before outputs are trusted.

The final outputs are Tableau-ready CSVs: statistical profiles of the ALICE population, year-over-year trend data, and side-by-side comparisons of the full vs. non-student population.

---

## What Gets Produced

- ALICE household counts and statistical profiles for 2019, 2021, 2022, and 2023
- Non-student ALICE population isolated for clean community analysis
- Year-over-year comparison of income, household size, and threshold proximity
- Validated, Tableau-ready exports for community stakeholder dashboards

---

## Tech Stack

Python · PostgreSQL · pandas · SQLAlchemy · Census API · JupyterLab · Tableau

---

## Replication

For full setup, pipeline steps, and SQL details: **[docs/REPLICATION.md](docs/REPLICATION.md)**
