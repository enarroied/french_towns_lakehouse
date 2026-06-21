from generate_reports.queries._loader import execute_sql


def get_departments(conn):
    return execute_sql(conn, "departments")


def get_city_data(conn, department_code, limit=None):
    return execute_sql(
        conn,
        "city_data",
        [department_code, department_code],
        limit=limit,
    )


def get_population_timeseries(conn, commune_id):
    return execute_sql(conn, "population_timeseries", [commune_id])


def get_population_history(conn, commune_id):
    return execute_sql(conn, "population_history", [commune_id])


def get_salary_timeseries(conn, commune_id):
    return execute_sql(conn, "salary_timeseries", [commune_id])


def get_salary_history(conn, commune_id):
    return execute_sql(conn, "salary_history", [commune_id])
