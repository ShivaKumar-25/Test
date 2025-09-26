import re
import json
from pathlib import Path

def sql_to_databricks_notebook(sql_file: str) -> str:
    """
    Converts a SQL file into a Databricks Python notebook (.ipynb) with %sql cells.
    
    Args:
        sql_file (str): Path to the SQL file.
        
    Returns:
        str: Path to the created Databricks notebook.
    """
    try:
        notebook_name = Path(sql_file).stem
        output_file = f"{notebook_name}.ipynb"

        # Read SQL file
        try:
            with open(sql_file, "r") as f:
                content = f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"SQL file '{sql_file}' not found.")

        # Split statements by semicolon
        statements = [stmt.strip() for stmt in content.split(";") if stmt.strip()]

        cells = []
        for stmt in statements:
            # Extract view name for title
            match = re.search(r"create\s+or\s+replace\s+temp\s+view\s+(\w+)", stmt, re.IGNORECASE)
            view_name = match.group(1) if match else "SQL Query"

            # Add %sql magic command
            sql_content = f"%sql\n{stmt} ;"

            cell = {
                "cell_type": "code",
                "source": [sql_content + "\n"],
                "metadata": {
                    "application/vnd.databricks.v1+cell": {
                        "showTitle": True,
                        "cellMetadata": {
                            "rowLimit": 10000,
                            "byteLimit": 2048000
                        },
                        "nuid": f"cell-{hash(stmt) % 1000000:06d}",
                        "inputWidgets": {},
                        "title": view_name
                    },
                    "databricks": {
                        "notebookMetadata": {
                            "inputWidgets": {},
                            "title": view_name
                        }
                    },
                    "title": view_name
                },
                "outputs": [],
                "execution_count": None
            }
            cells.append(cell)

        notebook = {
            "cells": cells,
            "metadata": {
                "application/vnd.databricks.v1+notebook": {
                    "notebookName": notebook_name,
                    "dashboards": [],
                    "notebookMetadata": {
                        "pythonIndentUnit": 4
                    },
                    "language": "python",
                    "widgets": {},
                    "notebookOrigID": 0
                },
                "language_info": {
                    "name": "python"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 5
        }

        # Save notebook
        with open(output_file, "w") as f:
            json.dump(notebook, f, indent=2)

        print(f"Databricks notebook created: {output_file}")
        print(f"Created {len(cells)} cells with titles: {[cell['metadata']['title'] for cell in cells]}")
        return output_file

    except Exception as e:
        print(f"Error creating notebook from '{sql_file}': {e}")
        raise

# Example usage
sql_to_databricks_notebook("test.sql")