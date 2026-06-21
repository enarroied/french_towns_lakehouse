import logging
import sys
from multiprocessing import Pool
from pathlib import Path

import click
from generate_reports.config import download_montserrat
from generate_reports.queries import get_city_data
from generate_reports.queries import get_departments
from generate_reports.renderer import generate_city_slides
from generate_reports.renderer import generate_dept_pdf
from generate_reports.utils import setup_duckdb_connection
from tqdm import tqdm


logger = logging.getLogger(__name__)


def _process_city(args: dict) -> str:
    conn = setup_duckdb_connection()
    try:
        generate_city_slides(conn, args["row"], args["output_dir"])
        return f"{args['row']['name']} ({args['row']['id']})"
    finally:
        conn.close()


def _resolve_departments(
    conn, department: tuple[str, ...], all_departments: bool
) -> list[tuple[str, str]]:
    depts_df = get_departments(conn)
    if all_departments:
        return list(
            zip(depts_df["department_code"], depts_df["department_name"], strict=False)
        )

    departments = []
    for code in department:
        match = depts_df[depts_df["department_code"] == code]
        if match.empty:
            click.echo(f"Warning: Department '{code}' not found, skipping", err=True)
            continue
        departments.append((code, match.iloc[0]["department_name"]))
    return departments


def _collect_slides(dept_code: str, out_path: Path) -> list[Path]:
    slide_paths = sorted(out_path.glob(f"{dept_code}/*_slide1.png"))
    city_slides: list[Path] = []
    for sp in slide_paths:
        commune_id = sp.stem.replace("_slide1", "")
        city_slides.extend(
            sorted(out_path.glob(f"{dept_code}/{commune_id}_slide*.png"))
        )
    return city_slides


def _run_parallel(pool_args: list[dict], workers: int) -> None:
    with Pool(workers) as pool:
        list(
            tqdm(
                pool.imap_unordered(_process_city, pool_args),
                total=len(pool_args),
                desc="  cities",
                unit="city",
            )
        )


def _run_sequential(rows: list[dict], output_dir: str) -> None:
    local_conn = setup_duckdb_connection()
    try:
        for r in tqdm(rows, desc="  cities", unit="city"):
            generate_city_slides(local_conn, r, output_dir)
    finally:
        local_conn.close()


def _process_department(
    conn,
    dept_code: str,
    dept_name: str,
    out_path: Path,
    limit: int | None,
    parallel: int,
) -> None:
    click.echo(f"\nProcessing department {dept_code} — {dept_name}")

    df = get_city_data(conn, dept_code, limit=limit)
    if df.empty:
        click.echo(f"  No data for department {dept_code}, skipping")
        return

    click.echo(f"  Cities to process: {len(df)}")

    dept_out = out_path / dept_code
    dept_out.mkdir(parents=True, exist_ok=True)

    rows = df.to_dict("records")
    pool_args = [{"row": r, "output_dir": str(out_path)} for r in rows]

    if parallel > 1 and len(rows) > 1:
        _run_parallel(pool_args, parallel)
    else:
        _run_sequential(rows, str(out_path))

    click.echo(f"  Generating PDF summary for {dept_name}")
    pdf_path = out_path / f"{dept_code}_summary.pdf"
    city_slides = _collect_slides(dept_code, out_path)
    generate_dept_pdf(city_slides, df, dept_name, pdf_path)

    click.echo(f"  Done: {len(df)} cities processed, {len(rows) * 6} slides generated")


def _run(
    department: tuple[str, ...],
    all_departments: bool,
    output_dir: str,
    limit: int | None,
    parallel: int,
) -> None:
    download_montserrat()
    out_path = Path(output_dir)
    conn = setup_duckdb_connection()

    try:
        departments = _resolve_departments(conn, department, all_departments)

        if not departments:
            click.echo("No valid departments to process", err=True)
            sys.exit(1)

        for dept_code, dept_name in departments:
            _process_department(conn, dept_code, dept_name, out_path, limit, parallel)

    finally:
        conn.close()

    click.echo("\nAll departments processed successfully.")


@click.command()
@click.option(
    "--department",
    "-d",
    multiple=True,
    type=str,
    help="Department code(s) to process (e.g. 75, 33)",
)
@click.option(
    "--all-departments",
    is_flag=True,
    help="Process all departments",
)
@click.option(
    "--output-dir",
    default="output",
    show_default=True,
    help="Output directory for generated files",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of cities per department (for testing)",
)
@click.option(
    "--parallel",
    type=int,
    default=4,
    show_default=True,
    help="Number of parallel workers",
)
def main(
    department: tuple[str, ...],
    all_departments: bool,
    output_dir: str,
    limit: int | None,
    parallel: int,
) -> None:
    if not department and not all_departments:
        click.echo(
            "Error: Specify at least one --department or use --all-departments",
            err=True,
        )
        sys.exit(1)

    _run(department, all_departments, output_dir, limit, parallel)


if __name__ == "__main__":
    main()
