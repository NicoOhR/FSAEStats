import os

import duckdb

con = duckdb.connect("race.duckdb")

csv_directory = "."

for filename in os.listdir(csv_directory):
    if filename.endswith(".csv"):
        table_name = os.path.splitext(filename)[0]
        file_path = os.path.join(csv_directory, filename)
        con.execute(
            f"""
            CREATE TABLE "{table_name}" AS
            SELECT * FROM read_csv_auto('{file_path}');
        """
        )
