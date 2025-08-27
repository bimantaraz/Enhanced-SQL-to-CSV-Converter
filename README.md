## Description

A robust Python script to convert SQL dump files—with advanced support for both MySQL (INSERT) and PostgreSQL (COPY) formats—into clean CSV files. Features include improved debugging, reliable multi-line parsing, table structure analysis, optional table filtering, and comprehensive error handling. Effortlessly exports data for further processing or analysis.

---

## Installation

### Prerequisites
- **Python 3.6 or newer**

### Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone https://github.com/bimantaraz/enhanced-sql-to-csv-converter.git
   cd enhanced-sql-to-csv-converter
   ```

2. **Create and activate a virtual environment (recommended):**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
## Usage

Convert an SQL dump to CSV with the following command:

```bash
python teraz_sql.py input.sql
```

By default, the output will be `input.csv` in the same directory. You can customize settings:

- Specify a custom output file:
  ```bash
  python teraz_sql.py input.sql output.csv
  ```
- Enable debug mode to see parsing steps and issues:
  ```bash
  python teraz_sql.py input.sql -d
  ```
- Filter for a specific table (only export rows from one table):
  ```bash
  python teraz_sql.py input.sql -t users
  ```
- Only analyze the file (show table structure, estimated row counts, etc.):
  ```bash
  python teraz_sql.py input.sql --analyze-only
  ```

### Advanced

- Works with both MySQL (INSERT INTO) and PostgreSQL (COPY FROM) statements.
- Handles large multi-line statements efficiently.
- Provides progress and error messages during processing.
- For more information or troubleshooting, try adding the `-d` (debug) flag.

---

## Example

### Command
```bash
python teraz_sql.py sample_dump.sql -d
```

### Console Output
```
🔄 Enhanced SQL to CSV Converter
==================================================
📂 Input file: example.com.sql
📄 Output file: example.com.csv
==================================================

🔍 Analyzing file structure...
📊 File Analysis:
   📄 Total lines: 2,529
   🏗️  CREATE TABLE statements: 1
   📥 INSERT statements for tables: ['users']
   📋 COPY statements for tables: []
   📈 Estimated data rows: 2,946,547
   ✅ Detected format: MYSQL

🔄 Converting file: example.com.sql
✅ Format confirmed: MYSQL
📋 Header untuk tabel 'users': 51 kolom
✅ Conversion completed!
📈 Total data rows written: 2,126,328
📄 Output file: example.com.csv
📊 Output file size: 1,036,956 bytes

🎉 Success! Data converted to example.com.csv
```

### Output CSV Format
**example.com.csv**
```csv
table_name,id,name,email,created_at,order_id,user_id,total,order_date,product_id,price,category
users,1,John Doe,john@email.com,2023-01-15,,,,,,,,
users,2,Jane Smith,jane@email.com,2023-01-16,,,,,,,,
orders,,,,1,1,99.99,2023-01-20,,,,
orders,,,,2,1,149.50,2023-01-21,,,,
products,,,,,,,1,Widget,29.99,Electronics
products,,,,,,,2,Gadget,49.99,Electronics
```

### Analysis Only Output
```bash
python teraz_sql.py sample_dump.sql --analyze-only
```

```
🔄 Enhanced SQL to CSV Converter
==================================================
📂 Input file: example.com.sql
📄 Output file: example.com.csv
🔍 Debug mode: enabled

🔍 Analyzing file structure...
🔍 DEBUG: Found INSERT for table: users
📊 File Analysis:
   📄 Total lines: 2,529
   🏗️  CREATE TABLE statements: 1
   📥 INSERT statements for tables: ['users']
   📋 COPY statements for tables: []
   📈 Estimated data rows: 2,946,547
   ✅ Detected format: MYSQL

🔄 Converting file: example.com.sql
🔍 DEBUG: Found CREATE TABLE for: users
🔍 DEBUG: Table users columns: ['users', 'email', 'encrypted_password']
🔍 DEBUG: Found INSERT for table: users
✅ Format confirmed: MYSQL
```
