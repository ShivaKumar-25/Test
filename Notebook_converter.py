import re
from pathlib import Path
import sqlparse
import json

def split_ctes_to_cells(sql_query: str, final_view: str = "final_output", keep_identifiers=True):
    # Normalize spaces and newlines
    query = " ".join(sql_query.strip().splitlines())

    # Regex to extract each CTE: name + body
    cte_pattern = r"(\w+)\s+AS\s*\((.*?)\)(?:,|$)"
    ctes = re.findall(cte_pattern, query, re.IGNORECASE | re.DOTALL)

    # Extract final SELECT (after last CTE)
    final_select_match = re.search(r"\)\s*SELECT(.*);?$", query, re.IGNORECASE | re.DOTALL)
    final_select = "SELECT" + final_select_match.group(1).strip() if final_select_match else None

    cells = []

    # Convert each CTE into CREATE OR REPLACE TEMP VIEW cell
    for name, body in ctes:
        cell_sql = f"CREATE OR REPLACE TEMP VIEW {name} AS\n{body.strip()};"
        formatted_sql = sqlparse.format(
            cell_sql,
            reindent=True,
            keyword_case="upper",
            identifier_case=None if keep_identifiers else "lower"
        ).strip()
        cells.append(formatted_sql)

    # Add final SELECT as last cell
    if final_select:
        final_sql = f"CREATE OR REPLACE TEMP VIEW {final_view} AS\n{final_select}"
        formatted_sql = sqlparse.format(
            final_sql,
            reindent=True,
            keyword_case="upper",
            identifier_case=None if keep_identifiers else "lower"
        ).strip()
        cells.append(formatted_sql)

    return cells


def save_as_notebook(cells, output_file: str):
    """Save list of SQL strings as a Jupyter notebook (.ipynb) with %sql magic"""
    notebook = {
        "cells": [],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    for sql in cells:
        cell = {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [f"%sql\n{sql}"]
        }
        notebook["cells"].append(cell)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2)

    print(f"✅ Notebook written to: {output_file}")


def process_sql_file(input_file: str, output_file: str, keep_identifiers=True):
    sql_query = Path(input_file).read_text()
    cells = split_ctes_to_cells(sql_query, keep_identifiers=keep_identifiers)
    save_as_notebook(cells, output_file)


# Example usage
if __name__ == "__main__":
    input_file = "input_file" # input SQL
    output_file = "notebook.ipynb"  # Databricks/Jupyter notebook
    process_sql_file(input_file, output_file, keep_identifiers=True)
