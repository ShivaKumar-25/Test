#!/usr/bin/env python3
"""
SQL Server to Databricks DDL Converter
Converts SQL Server table DDL statements to Databricks Delta table format
"""

import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional


class SQLServerToDatabricksDDLConverter:
    """Convert SQL Server DDL statements to Databricks DDL"""
    
    def __init__(self):
        # SQL Server to Databricks data type mapping
        self.type_mapping = {
            # Numeric types
            'int': 'INT',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'TINYINT',
            'bit': 'BOOLEAN',
            'decimal': 'DECIMAL',
            'numeric': 'DECIMAL',
            'float': 'DOUBLE',
            'real': 'FLOAT',
            'money': 'DECIMAL(19,4)',
            'smallmoney': 'DECIMAL(10,4)',
            
            # String types
            'char': 'STRING',
            'varchar': 'STRING',
            'text': 'STRING',
            'nchar': 'STRING',
            'nvarchar': 'STRING',
            'ntext': 'STRING',
            
            # Date/Time types
            'date': 'DATE',
            'datetime': 'TIMESTAMP',
            'datetime2': 'TIMESTAMP',
            'datetimeoffset': 'TIMESTAMP',
            'smalldatetime': 'TIMESTAMP',
            'time': 'STRING',  # Databricks doesn't have TIME type
            'timestamp': 'BINARY',  # SQL Server timestamp is binary
            
            # Binary types
            'binary': 'BINARY',
            'varbinary': 'BINARY',
            'image': 'BINARY',
            
            # Other types
            'uniqueidentifier': 'STRING',
            'xml': 'STRING',
            'sql_variant': 'STRING',
            'geography': 'STRING',
            'geometry': 'STRING',
            'hierarchyid': 'STRING'
        }
        
        self.tables = []
        
    def parse_sql_server_ddl(self, content: str) -> List[Dict]:
        """Parse SQL Server DDL content and extract table definitions"""
        tables = []
        
        # Remove GO statements and comments
        content = re.sub(r'^GO\s*$', '', content, flags=re.MULTILINE | re.IGNORECASE)
        content = re.sub(r'^USE\s+.*$', '', content, flags=re.MULTILINE | re.IGNORECASE)
        content = re.sub(r'^SET\s+.*$', '', content, flags=re.MULTILINE | re.IGNORECASE)
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
        
        # Split by CREATE TABLE statements
        create_table_pattern = r'CREATE\s+TABLE\s+'
        tables_raw = re.split(create_table_pattern, content, flags=re.IGNORECASE)
        
        for table_raw in tables_raw[1:]:  # Skip first empty split
            table_info = self._parse_single_table(table_raw)
            if table_info:
                tables.append(table_info)
        
        return tables
    
    def _parse_single_table(self, table_content: str) -> Optional[Dict]:
        """Parse a single table definition"""
        # Extract table name
        table_name_match = re.match(r'\[?([^\[\]]+)\]?\.\[?([^\[\]]+)\]?\s*\(', table_content)
        if not table_name_match:
            # Try without schema
            table_name_match = re.match(r'\[?([^\[\]]+)\]?\s*\(', table_content)
            if not table_name_match:
                return None
            schema_name = None
            table_name = table_name_match.group(1)
        else:
            schema_name = table_name_match.group(1)
            table_name = table_name_match.group(2)
        
        # Find the table definition boundaries more carefully
        # Look for the CREATE TABLE ... ( ... ) pattern, handling nested parentheses
        paren_depth = 0
        start_idx = -1
        end_idx = -1
        
        for i, char in enumerate(table_content):
            if char == '(':
                if paren_depth == 0 and start_idx == -1:
                    start_idx = i + 1
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
                if paren_depth == 0 and start_idx != -1:
                    end_idx = i
                    break
        
        if start_idx == -1 or end_idx == -1:
            return None
            
        columns_raw = table_content[start_idx:end_idx]
        
        # Remove CONSTRAINT blocks - match full constraint definitions
        # Remove CONSTRAINT definitions including PRIMARY KEY CLUSTERED and WITH clauses
        columns_raw = re.sub(
            r',?\s*CONSTRAINT\s+\[?[^\]]*\]?\s+(PRIMARY KEY|UNIQUE|FOREIGN KEY|CHECK).*?(?:WITH\s*\([^)]*\))?.*?(?=,\s*\[|$)', 
            '', columns_raw, flags=re.DOTALL | re.IGNORECASE)
        
        # Parse columns
        columns = self._parse_columns(columns_raw)
        
        return {
            'schema': schema_name,
            'name': table_name,
            'columns': columns
        }
    
    def _parse_columns(self, columns_raw: str) -> List[Dict]:
        """Parse column definitions from raw column string"""
        columns = []
        
        # Split by commas (but not within parentheses)
        column_lines = self._smart_split(columns_raw)
        
        for line in column_lines:
            line = line.strip()
            if not line:
                continue
            
            # Skip constraints and their options
            if re.match(r'^\s*(CONSTRAINT|PRIMARY|UNIQUE|FOREIGN|CHECK|PAD_INDEX|STATISTICS_NORECOMPUTE|IGNORE_DUP_KEY|ALLOW_ROW_LOCKS|ALLOW_PAGE_LOCKS)', line, re.IGNORECASE):
                continue
            
            # Skip lines that look like constraint options
            if '=' in line and any(opt in line.upper() for opt in ['PAD_INDEX', 'STATISTICS_NORECOMPUTE', 'IGNORE_DUP_KEY', 'ALLOW_ROW_LOCKS', 'ALLOW_PAGE_LOCKS']):
                continue
                
            # Parse column: [column_name] [data_type] [NULL/NOT NULL]
            # Handle various patterns including types with parameters, including numeric(19,0)
            column_match = re.match(r'\[?([^\[\]]+)\]?\s+\[?([a-zA-Z_]+)(?:\(([^)]+)\))?\]?\s*(.*)', line)
            if column_match:
                col_name = column_match.group(1)
                col_type = column_match.group(2).lower()
                col_params = column_match.group(3)
                col_modifiers = column_match.group(4) if column_match.group(4) else ''
                
                # Check for NULL/NOT NULL
                nullable = 'NOT NULL' not in col_modifiers.upper()
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'params': col_params,
                    'nullable': nullable
                })
        
        return columns
    
    def _smart_split(self, text: str) -> List[str]:
        """Split by commas but not within parentheses"""
        parts = []
        current = []
        paren_depth = 0
        
        for char in text:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                parts.append(''.join(current).strip())
                current = []
                continue
            current.append(char)
        
        if current:
            parts.append(''.join(current).strip())
        
        return parts
    
    def convert_data_type(self, sql_server_type: str, params: Optional[str] = None) -> str:
        """Convert SQL Server data type to Databricks data type"""
        base_type = sql_server_type.lower()
        
        # Handle special cases with parameters
        if base_type in ['decimal', 'numeric'] and params:
            return f'DECIMAL({params})'
        elif base_type in ['char', 'varchar', 'nchar', 'nvarchar']:
            # Databricks uses STRING for all character types
            return 'STRING'
        elif base_type in ['float'] and params:
            # SQL Server float(n) where n < 25 is REAL (FLOAT in Databricks)
            try:
                n = int(params)
                if n < 25:
                    return 'FLOAT'
                else:
                    return 'DOUBLE'
            except:
                return 'DOUBLE'
        
        # Default mapping
        return self.type_mapping.get(base_type, 'STRING')
    
    def generate_databricks_ddl(self, table: Dict, use_catalog: bool = True) -> str:
        """Generate Databricks Delta table DDL from parsed table info"""
        ddl_parts = []
        
        # Table name with optional catalog/schema
        if use_catalog and table.get('schema'):
            full_table_name = f"{table['schema']}.{table['name']}"
        else:
            full_table_name = table['name']
        
        ddl_parts.append(f"CREATE TABLE IF NOT EXISTS {full_table_name} (")
        
        # Add columns
        column_defs = []
        for col in table['columns']:
            databricks_type = self.convert_data_type(col['type'], col.get('params'))
            null_clause = '' if col['nullable'] else ' NOT NULL'
            column_defs.append(f"  {col['name']} {databricks_type}{null_clause}")
        
        ddl_parts.append(',\n'.join(column_defs))
        ddl_parts.append(")")
        ddl_parts.append("USING DELTA;")
        
        return '\n'.join(ddl_parts)
    
    def convert_file(self, input_file: str, output_file: str = None, use_catalog: bool = True) -> None:
        """Convert SQL Server DDL file to Databricks DDL"""
        # Read input file
        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        with open(input_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Parse SQL Server DDL
        tables = self.parse_sql_server_ddl(content)
        
        if not tables:
            print("No tables found in the input file.")
            return
        
        # Generate Databricks DDL
        databricks_ddls = []
        for table in tables:
            ddl = self.generate_databricks_ddl(table, use_catalog)
            databricks_ddls.append(f"-- Table: {table['name']}")
            databricks_ddls.append(ddl)
            databricks_ddls.append("")  # Empty line between tables
        
        output_content = '\n'.join(databricks_ddls)
        
        # Write to output file
        if output_file:
            output_path = Path(output_file)
        else:
            # Generate output filename
            output_path = input_path.parent / f"{input_path.stem}_databricks.sql"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(output_content)
        
        print(f"Conversion complete!")
        print(f"Input file: {input_file}")
        print(f"Output file: {output_path}")
        print(f"Total tables converted: {len(tables)}")
        
        # Print summary
        print("\nTables converted:")
        for table in tables:
            schema_prefix = f"{table['schema']}." if table.get('schema') else ""
            print(f"  - {schema_prefix}{table['name']} ({len(table['columns'])} columns)")


def main():
    """Main entry point"""
    converter = SQLServerToDatabricksDDLConverter()
    
    # Default input file
    input_file = "Sql Server Tbale ddls.sql"
    output_file = None  # Will auto-generate
    
    # Check command line arguments
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    try:
        converter.convert_file(input_file, output_file)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()