import re
import csv
import sys
import os
import argparse
from typing import List, Optional, Tuple, Dict

class SQLParser:
    def __init__(self, input_file: str, output_file: str, debug: bool = False, table_filter: str = None):
        self.input_file = input_file
        self.output_file = output_file
        self.debug = debug
        self.table_filter = table_filter
        self.parser_mode = None
        self.in_data_block = False
        self.headers = []
        self.total_rows = 0
        self.current_table = None
        self.table_structures = {}
        self.insert_statements_found = []
        
    def debug_print(self, message: str):
        if self.debug:
            print(f"🔍 DEBUG: {message}")
    
    def detect_format(self, line: str) -> Optional[str]:
        line = line.strip()
        
        insert_patterns = [
            r'INSERT\s+INTO\s+`?(\w+)`?\s+VALUES\s*\(',
            r'INSERT\s+INTO\s+`?(\w+)`?\s*\([^)]+\)\s+VALUES\s*\(',
            r'INSERT\s+INTO\s+`?(\w+)`?\s+\([^)]+\)\s+VALUES\s*\('
        ]
        
        for pattern in insert_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                match = re.search(r'INSERT\s+INTO\s+`?(\w+)`?', line, re.IGNORECASE)
                if match:
                    table_name = match.group(1)
                    self.debug_print(f"Found INSERT for table: {table_name}")
                    self.insert_statements_found.append(table_name)
                return 'mysql'
        
        if re.search(r'COPY\s+(?:\w+\.)?\w+\s*\([^)]+\)\s+FROM\s+stdin;', line, re.IGNORECASE):
            return 'postgresql'
            
        return None
    
    def parse_create_table(self, line: str, next_lines: List[str]) -> Optional[str]:
        create_match = re.search(r'CREATE\s+TABLE\s+`?(\w+)`?', line, re.IGNORECASE)
        if not create_match:
            return None
            
        table_name = create_match.group(1)
        self.debug_print(f"Found CREATE TABLE for: {table_name}")
        
        full_create = line
        paren_count = line.count('(') - line.count(')')
        
        line_idx = 0
        while paren_count > 0 and line_idx < len(next_lines):
            next_line = next_lines[line_idx]
            full_create += " " + next_line.strip()
            paren_count += next_line.count('(') - next_line.count(')')
            line_idx += 1
        
        columns = []
        column_pattern = r'`(\w+)`\s+[^,\)]+(?:,|\s*\))'
        matches = re.findall(column_pattern, full_create)
        
        if matches:
            self.table_structures[table_name] = matches
            self.debug_print(f"Table {table_name} columns: {matches}")
        
        return table_name
    
    def parse_mysql_insert_improved(self, line: str, writer: csv.writer) -> int:
        self.debug_print(f"Parsing INSERT line (first 200 chars): {line[:200]}...")
        
        insert_pattern = r'INSERT\s+INTO\s+`?(\w+)`?(?:\s*\([^)]+\))?\s+VALUES\s+(.*?)(?:;|$)'
        
        match = re.search(insert_pattern, line, re.IGNORECASE | re.DOTALL)
        if not match:
            self.debug_print("No INSERT pattern match found")
            return 0
            
        table_name = match.group(1)
        values_str = match.group(2)
        
        self.debug_print(f"Processing INSERT for table: {table_name}")
        self.debug_print(f"Values string length: {len(values_str)}")
        
        if self.table_filter and self.table_filter.lower() not in table_name.lower():
            self.debug_print(f"Skipping table {table_name} due to filter")
            return 0
        
        if not self.headers:
            if table_name in self.table_structures:
                self.headers = self.table_structures[table_name]
                self.debug_print(f"Using table structure for headers: {self.headers}")
            else:
                column_match = re.search(r'INSERT\s+INTO\s+`?\w+`?\s*\(([^)]+)\)', line, re.IGNORECASE)
                if column_match:
                    columns_str = column_match.group(1)
                    self.headers = [col.strip('` "\'') for col in columns_str.split(',')]
                    self.debug_print(f"Extracted headers from INSERT: {self.headers}")
                else:
                    self.debug_print("No column list found in INSERT, using table structure if available")
                    if table_name in self.table_structures:
                        self.headers = self.table_structures[table_name]
                    else:
                        self.debug_print("No headers available - will try to guess from data")
            
            if self.headers:
                writer.writerow(self.headers)
                print(f"📋 Header untuk tabel '{table_name}': {len(self.headers)} kolom")
        
        rows_written = 0
        
        if values_str.strip().endswith(';'):
            values_str = values_str.strip()[:-1]
        
        self.debug_print(f"Cleaned values string length: {len(values_str.strip())}")
        
        if len(values_str) > 100000:  
            self.debug_print("Large INSERT detected, using chunk-based parsing")
            return self.parse_large_insert(values_str, writer, table_name)
        
        value_sets = []
        current_set = ""
        paren_depth = 0
        in_string = False
        string_char = None
        
        i = 0
        while i < len(values_str):
            char = values_str[i]
            
            if not in_string:
                if char in ["'", '"']:
                    in_string = True
                    string_char = char
                elif char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                    if paren_depth == 0:
                        current_set += char
                        value_sets.append(current_set.strip())
                        current_set = ""
                        while i + 1 < len(values_str) and values_str[i + 1] in ', \t\n\r':
                            i += 1
                        i += 1
                        continue
            else:
                if char == string_char:
                    if i + 1 < len(values_str) and values_str[i + 1] == string_char:
                        current_set += char + string_char
                        i += 1
                    elif char == '\\' and i + 1 < len(values_str):
                        current_set += char + values_str[i + 1]
                        i += 1
                    else:
                        in_string = False
                        string_char = None
            
            current_set += char
            i += 1
        
        if current_set.strip() and paren_depth == 0:
            value_sets.append(current_set.strip())
        
        self.debug_print(f"Found {len(value_sets)} value sets")
        
        if len(value_sets) == 0:
            self.debug_print("No value sets found - trying alternative parsing")
            alt_pattern = r'\([^)]*\)'
            alt_matches = re.findall(alt_pattern, values_str)
            self.debug_print(f"Alternative parsing found {len(alt_matches)} matches")
            value_sets = alt_matches
        
        for idx, value_set in enumerate(value_sets):
            if not value_set.strip():
                continue
                
            if not value_set.startswith('('):
                value_set = '(' + value_set
            if not value_set.endswith(')'):
                value_set = value_set + ')'
                
            values_content = value_set[1:-1]
            values = self.parse_csv_values_improved(values_content)
            
            if values:
                if not self.headers:
                    self.headers = [f"column_{i+1}" for i in range(len(values))]
                    writer.writerow(self.headers)
                    self.debug_print(f"Generated headers: {self.headers}")
                
                if self.headers and len(values) != len(self.headers):
                    self.debug_print(f"Column mismatch: expected {len(self.headers)}, got {len(values)}")
                    if len(values) < len(self.headers):
                        values.extend([''] * (len(self.headers) - len(values)))
                    else:
                        values = values[:len(self.headers)]
                
                cleaned_values = []
                for val in values:
                    val = val.strip()
                    if val.upper() == 'NULL':
                        cleaned_values.append('')
                    elif val.startswith("'") and val.endswith("'") and len(val) > 1:
                        unquoted = val[1:-1].replace("''", "'").replace("\\'", "'")
                        cleaned_values.append(unquoted)
                    elif val.startswith('"') and val.endswith('"') and len(val) > 1:
                        unquoted = val[1:-1].replace('""', '"').replace('\\"', '"')
                        cleaned_values.append(unquoted)
                    else:
                        cleaned_values.append(val)
                
                writer.writerow(cleaned_values)
                rows_written += 1
                
                if rows_written <= 5 or self.debug:  
                    self.debug_print(f"Row {rows_written}: {cleaned_values[:3]}...")
                    
                if rows_written % 1000 == 0:
                    self.debug_print(f"Processed {rows_written} rows from current INSERT")
        
        self.debug_print(f"INSERT parsing completed: {rows_written} rows written")
        return rows_written
    
    def parse_large_insert(self, values_str: str, writer: csv.writer, table_name: str) -> int:
        self.debug_print("Processing large INSERT statement in chunks")
        
        rows_written = 0
        buffer = ""
        paren_depth = 0
        in_string = False
        string_char = None
        
        for char in values_str:
            buffer += char
            
            if not in_string:
                if char in ["'", '"']:
                    in_string = True
                    string_char = char
                elif char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                    if paren_depth == 0:
                        if buffer.strip().startswith('(') and buffer.strip().endswith(')'):
                            values_content = buffer.strip()[1:-1]
                            values = self.parse_csv_values_improved(values_content)
                            
                            if values:
                                if not self.headers:
                                    self.headers = [f"column_{i+1}" for i in range(len(values))]
                                    writer.writerow(self.headers)
                                
                                cleaned_values = []
                                for val in values:
                                    val = val.strip()
                                    if val.upper() == 'NULL':
                                        cleaned_values.append('')
                                    elif val.startswith("'") and val.endswith("'") and len(val) > 1:
                                        unquoted = val[1:-1].replace("''", "'").replace("\\'", "'")
                                        cleaned_values.append(unquoted)
                                    else:
                                        cleaned_values.append(val)
                                
                                writer.writerow(cleaned_values)
                                rows_written += 1
                                
                                if rows_written % 5000 == 0:
                                    print(f"   📊 Processed {rows_written:,} rows from large INSERT...")
                        
                        buffer = ""
            else:
                if char == string_char and buffer.endswith('\\' + char):
                    pass
                elif char == string_char:
                    in_string = False
                    string_char = None
        
        self.debug_print(f"Large INSERT processing completed: {rows_written} rows")
        return rows_written
    
    def parse_csv_values_improved(self, values_str: str) -> List[str]:
        result = []
        current_value = ""
        in_quote = False
        quote_char = None
        i = 0
        
        while i < len(values_str):
            char = values_str[i]
            
            if not in_quote:
                if char in ["'", '"']:
                    in_quote = True
                    quote_char = char
                    current_value += char
                elif char == ',':
                    result.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            else:
                current_value += char
                if char == quote_char:
                    if i + 1 < len(values_str) and values_str[i + 1] == quote_char:
                        current_value += quote_char
                        i += 1  
                    else:
                        in_quote = False
                        quote_char = None
            
            i += 1
        
        if current_value.strip():
            result.append(current_value.strip())
        
        return result
    
    def parse_postgresql_copy(self, line: str, writer: csv.writer) -> int:
        line = line.strip()
        
        copy_pattern = r'COPY\s+(?:(\w+)\.)?(\w+)\s+\(([^)]+)\)\s+FROM\s+stdin;'
        copy_match = re.search(copy_pattern, line, re.IGNORECASE)
        
        if copy_match:
            schema = copy_match.group(1)
            table = copy_match.group(2)
            columns_str = copy_match.group(3)
            
            self.debug_print(f"COPY statement for table: {table}")
            self.in_data_block = True
            self.current_table = table
            
            if not self.headers:
                self.headers = [col.strip() for col in columns_str.split(',')]
                writer.writerow(self.headers)
                print(f"📋 Header PostgreSQL untuk '{table}': {len(self.headers)} kolom")
            return 0
        
        if line == '\\.':
            self.in_data_block = False
            self.debug_print("End of COPY data block")
            return 0
        
        if self.in_data_block and line:
            values = line.split('\t')
            
            cleaned_values = []
            for val in values:
                if val == '\\N':
                    cleaned_values.append('')
                else:
                    val = val.replace('\\t', '\t').replace('\\n', '\n').replace('\\r', '\r')
                    val = val.replace('\\\\', '\\')
                    cleaned_values.append(val)
            
            if len(cleaned_values) == len(self.headers):
                writer.writerow(cleaned_values)
                return 1
            else:
                self.debug_print(f"Column mismatch: expected {len(self.headers)}, got {len(cleaned_values)}")
        
        return 0
    
    def analyze_file(self) -> Dict:
        analysis = {
            'total_lines': 0,
            'create_tables': [],
            'insert_statements': [],
            'copy_statements': [],
            'detected_format': None,
            'estimated_data_rows': 0
        }
        
        print("🔍 Analyzing file structure...")
        
        try:
            with open(self.input_file, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                analysis['total_lines'] = len(lines)
                
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if not stripped or stripped.startswith('--'):
                        continue
                    
                    if re.search(r'CREATE\s+TABLE', stripped, re.IGNORECASE):
                        match = re.search(r'CREATE\s+TABLE\s+`?(\w+)`?', stripped, re.IGNORECASE)
                        if match:
                            analysis['create_tables'].append(match.group(1))
                    
                    if re.search(r'INSERT\s+INTO', stripped, re.IGNORECASE):
                        match = re.search(r'INSERT\s+INTO\s+`?(\w+)`?', stripped, re.IGNORECASE)
                        if match:
                            table = match.group(1)
                            if table not in analysis['insert_statements']:
                                analysis['insert_statements'].append(table)
                            values_count = stripped.count('VALUES')
                            paren_pairs = min(stripped.count('('), stripped.count(')'))
                            analysis['estimated_data_rows'] += max(values_count, paren_pairs // 2)
                    
                    if re.search(r'COPY\s+(?:\w+\.)?\w+\s*\([^)]+\)\s+FROM\s+stdin;', stripped, re.IGNORECASE):
                        match = re.search(r'COPY\s+(?:\w+\.)?(\w+)\s*\(', stripped, re.IGNORECASE)
                        if match:
                            analysis['copy_statements'].append(match.group(1))
                    
                    if not analysis['detected_format']:
                        fmt = self.detect_format(stripped)
                        if fmt:
                            analysis['detected_format'] = fmt
        
        except Exception as e:
            print(f"❌ Error analyzing file: {e}")
        
        return analysis
    
    def convert(self) -> bool:
        if not os.path.exists(self.input_file):
            print(f"❌ File input '{self.input_file}' tidak ditemukan!")
            return False
        
        analysis = self.analyze_file()
        
        print(f"📊 File Analysis:")
        print(f"   📄 Total lines: {analysis['total_lines']:,}")
        print(f"   🏗️  CREATE TABLE statements: {len(analysis['create_tables'])}")
        print(f"   📥 INSERT statements for tables: {analysis['insert_statements']}")
        print(f"   📋 COPY statements for tables: {analysis['copy_statements']}")
        print(f"   📈 Estimated data rows: {analysis['estimated_data_rows']:,}")
        
        if analysis['detected_format']:
            print(f"   ✅ Detected format: {analysis['detected_format'].upper()}")
        else:
            print(f"   ❌ No INSERT or COPY statements detected!")
            return False
        
        if not analysis['insert_statements'] and not analysis['copy_statements']:
            print(f"❌ No data insertion statements found in the file!")
            print(f"   The file appears to contain only table structures.")
            print(f"   Look for files with INSERT INTO or COPY statements.")
            return False
        
        target_tables = analysis['insert_statements'] + analysis['copy_statements']
        if self.table_filter:
            target_tables = [t for t in target_tables if self.table_filter.lower() in t.lower()]
            if not target_tables:
                print(f"❌ No tables match filter '{self.table_filter}'")
                return False
            print(f"🎯 Filtering for tables: {target_tables}")
        
        try:
            with open(self.input_file, 'r', encoding='utf-8', errors='ignore') as infile:
                all_lines = infile.readlines()
            
            with open(self.output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
                
                print(f"\n🔄 Converting file: {self.input_file}")
                line_count = 0
                i = 0
                
                while i < len(all_lines):
                    line = all_lines[i]
                    line_count += 1
                    i += 1
                    
                    stripped_line = line.strip()
                    if not stripped_line or stripped_line.startswith('--') or stripped_line.startswith('/*'):
                        continue
                    
                    if re.search(r'CREATE\s+TABLE', stripped_line, re.IGNORECASE):
                        remaining_lines = []
                        j = i
                        while j < len(all_lines) and j < i + 100:
                            remaining_lines.append(all_lines[j])
                            if ');' in all_lines[j]:
                                break
                            j += 1
                        self.parse_create_table(stripped_line, remaining_lines)
                        continue
                    
                    if not self.parser_mode:
                        detected_format = self.detect_format(stripped_line)
                        if detected_format:
                            self.parser_mode = detected_format
                            print(f"✅ Format confirmed: {detected_format.upper()}")
                    
                    rows_added = 0
                    if self.parser_mode == 'mysql':
                        if 'INSERT INTO' in stripped_line.upper():
                            full_statement = stripped_line
                            j = i
                            while not full_statement.strip().endswith(';') and j < len(all_lines):
                                if j < len(all_lines):
                                    full_statement += " " + all_lines[j].strip()
                                    line_count += 1
                                    i += 1
                                    j += 1
                                else:
                                    break
                            
                            rows_added = self.parse_mysql_insert_improved(full_statement, writer)
                            
                    elif self.parser_mode == 'postgresql':
                        rows_added = self.parse_postgresql_copy(stripped_line, writer)
                    
                    self.total_rows += rows_added
                    
                    if line_count % 5000 == 0:
                        print(f"📊 Processed: {line_count:,} lines, {self.total_rows:,} data rows")
            
            print(f"✅ Conversion completed!")
            print(f"📈 Total data rows written: {self.total_rows:,}")
            print(f"📄 Output file: {self.output_file}")
            
            if os.path.exists(self.output_file):
                file_size = os.path.getsize(self.output_file)
                print(f"📊 Output file size: {file_size:,} bytes")
                
            return self.total_rows > 0
                
        except Exception as e:
            print(f"❌ Error during conversion: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return False

def main():
    parser = argparse.ArgumentParser(
        description="Enhanced SQL to CSV Converter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python x.py input.sql
  python x.py input.sql output.csv
  python x.py input.sql -d
  python x.py input.sql -t users
        """
    )
    
    parser.add_argument('input_file', help='Input SQL file')
    parser.add_argument('output_file', nargs='?', help='Output CSV file (default: input_file.csv)')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output')
    parser.add_argument('-t', '--table', help='Filter for specific table name')
    parser.add_argument('-a', '--analyze-only', action='store_true', help='Only analyze file, don\'t convert')
    
    args = parser.parse_args()
    
    if not args.output_file:
        base_name = os.path.splitext(args.input_file)[0]
        args.output_file = f"{base_name}.csv"
    
    print("🔄 Enhanced SQL to CSV Converter")
    print("=" * 50)
    print(f"📂 Input file: {args.input_file}")
    print(f"📄 Output file: {args.output_file}")
    if args.table:
        print(f"🎯 Table filter: {args.table}")
    if args.debug:
        print(f"🔍 Debug mode: enabled")
    print()
    
    sql_parser = SQLParser(args.input_file, args.output_file, args.debug, args.table)
    
    if args.analyze_only:
        analysis = sql_parser.analyze_file()
        print("\n📋 Analysis complete!")
        return
    
    success = sql_parser.convert()
    
    if success:
        print(f"\n🎉 Success! Data converted to {args.output_file}")
    else:
        print(f"\n💥 Conversion failed!")
        print(f"💡 Tips:")
        print(f"   - Check if file contains INSERT INTO or COPY statements")
        print(f"   - Try with --debug flag for more information")
        print(f"   - Use --analyze-only to inspect file structure")
        sys.exit(1)

if __name__ == "__main__":
    main()
