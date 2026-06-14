"""Export department-level population data from the gold layer to CSV."""

from pathlib import Path

import pandas as pd
from blog.blog_utils import _connect


OUTPUT = Path(__file__).resolve().parents[2] / "blog" / "data" / "departments.csv"


def main() -> None:
    conn = _connect()
    query = """
        with dpt as (
            select
                c.department_code,
                c.department_name,
                p.year,
                p.population
            from gold.dim_communes_france c
            join gold.fact_population p
                on c.id = p.id
            where c.is_current = true
        )
        select
            department_code,
            department_name,
            year,
            sum(population)::BIGINT as total_population
        from dpt
        group by all
        order by department_code, year
    """
    df: pd.DataFrame = conn.execute(query).fetchdf()

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT, index=False)
    print(f"✅ Exported {len(df)} rows → {OUTPUT}")


if __name__ == "__main__":
    main()
