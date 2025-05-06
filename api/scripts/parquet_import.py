#!/usr/bin/env python3
import logging
import os

import duckdb
import typer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

app = typer.Typer(help="CLI tool to import Parquet files into a DuckDB database.")


def import_parquet_to_duckdb(
    parquet_file_path: str, db_file_path: str, table_name: str
):
    """
    Import a Parquet file into a DuckDB database.

    Args:
        parquet_file_path (str): The path to the Parquet file.
        db_file_path (str): The path to the DuckDB database file.
        table_name (str): The name of the table to create or overwrite in the database.

    Raises:
        FileNotFoundError: If the Parquet file does not exist.
    """
    logging.info("Starting import of Parquet file: %s", parquet_file_path)

    # Check if the Parquet file exists
    if not os.path.exists(parquet_file_path):
        logging.error("Parquet file not found: %s", parquet_file_path)
        raise FileNotFoundError(f"Parquet file not found: {parquet_file_path}")

    # Connect to the DuckDB database (creates the file if it doesn't exist)
    logging.info("Connecting to DuckDB database: %s", db_file_path)
    conn = duckdb.connect(db_file_path)

    try:
        # Import the Parquet file into the specified table
        logging.info("Importing Parquet file into table '%s'", table_name)
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file_path}')"
        )
        typer.echo(
            f"Successfully imported '{parquet_file_path}' into table '{table_name}' in database '{db_file_path}'."
        )
        logging.info(
            "Successfully imported '%s' into table '%s' in database '%s'.",
            parquet_file_path,
            table_name,
            db_file_path,
        )
    except duckdb.Error as e:
        logging.error("DuckDB error while importing Parquet file: %s", e)
        typer.echo(f"DuckDB error while importing Parquet file: {e}", err=True)
    except OSError as e:
        logging.error("OS error while accessing files: %s", e)
        typer.echo(f"OS error while accessing files: {e}", err=True)
    finally:
        # Close the connection
        logging.info("Closing DuckDB connection.")
        conn.close()


@app.command()
def import_parquet(
    parquet_file: str = typer.Argument(..., help="Path to the Parquet file."),
    db_file: str = typer.Argument(..., help="Path to the DuckDB database file."),
    table: str = typer.Argument(..., help="Name of the table to create or overwrite."),
):
    """
    Import a Parquet file into a DuckDB database.
    """
    try:
        logging.info("CLI command invoked: import_parquet")
        import_parquet_to_duckdb(parquet_file, db_file, table)
    except FileNotFoundError as e:
        logging.error("FileNotFoundError: %s", e)
        typer.echo(f"Error: {e}", err=True)
    except duckdb.Error as e:
        logging.error("DuckDB error: %s", e)
        typer.echo(f"DuckDB error: {e}", err=True)
    except OSError as e:
        logging.error("OS error: %s", e)
        typer.echo(f"OS error: {e}", err=True)


if __name__ == "__main__":
    app()
