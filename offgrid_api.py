from fastapi import FastAPI, HTTPException
from OffGridDB import OffGridDB
from typing import Optional
from datetime import datetime
import os

app = FastAPI(title="OffGridDB API", description="API for managing off-grid cost data")

@app.post("/load")
async def load_json(json_path: str, db: str = "offgrid.db", drop: bool = False):
    """
    Load JSON data into the database.
    - json_path: Path to the JSON file.
    - db: Path to the SQLite database (default: offgrid.db).
    - drop: Drop existing tables before loading (default: False).
    """
    try:
        if not os.path.exists(json_path):
            raise HTTPException(status_code=400, detail="JSON file not found")
        
        db_instance = OffGridDB(db, log_file="offgrid.log")
        with db_instance:
            db_instance.load_json(json_path, drop_if_exists=drop)
        return {"message": f"Successfully loaded {json_path} into {db}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/query/{query_type}")
async def query_data(query_type: str, level: Optional[int] = None, db: str = "offgrid.db"):
    """
    Query the database.
    - query_type: Type of data to query (levels, monthly, fixed).
    - level: Filter by level ID (optional).
    - db: Path to the SQLite database (default: offgrid.db).
    """
    valid_query_types = ["levels", "monthly", "fixed"]
    if query_type not in valid_query_types:
        raise HTTPException(status_code=400, detail=f"Invalid query_type. Must be one of {valid_query_types}")

    try:
        db_instance = OffGridDB(db, log_file="offgrid.log")
        with db_instance:
            if query_type == "levels":
                query = "SELECT * FROM levels" + (f" WHERE level = {level}" if level else "")
            elif query_type == "monthly":
                query = "SELECT * FROM monthly_costs" + (f" WHERE level_id = {level}" if level else "")
            elif query_type == "fixed":
                query = "SELECT * FROM fixed_costs" + (f" WHERE level_id = {level}" if level else "")
            
            results = db_instance.query(query)
            return [dict(row) for row in results]  # Convert rows to dictionaries for JSON response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

@app.get("/report")
async def generate_report(level: Optional[int] = None, db: str = "offgrid.db", output: str = "report.md"):
    """
    Generate a cost report.
    - level: Generate report for a specific level (optional).
    - db: Path to the SQLite database (default: offgrid.db).
    - output: Output file for the report (default: report.md).
    """
    try:
        db_instance = OffGridDB(db, log_file="offgrid.log")
        with db_instance:
            query = """
                SELECT l.level, l.name, l.description, l.total_monthly, l.total_fixed,
                       GROUP_CONCAT(m.name || ': ' || m.amount) as monthly_costs,
                       GROUP_CONCAT(f.name || ': ' || f.total) as fixed_costs
                FROM levels l
                LEFT JOIN monthly_costs m ON l.level = m.level_id
                LEFT JOIN fixed_costs f ON l.level = f.level_id
            """
            if level:
                query += f" WHERE l.level = {level}"
            query += " GROUP BY l.level"
            
            results = db_instance.query(query)
            
            report = f"# OffGridDB Cost Report\n\nGenerated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            for row in results:
                report += f"## Level {row['level']}: {row['name']}\n"
                report += f"- **Description**: {row['description']}\n"
                report += f"- **Total Monthly Cost**: ${row['total_monthly']}\n"
                report += f"- **Total Fixed Cost**: ${row['total_fixed']}\n"
                report += f"- **Monthly Costs**: {row['monthly_costs'] or 'None'}\n"
                report += f"- **Fixed Costs**: {row['fixed_costs'] or 'None'}\n\n"
            
            with open(output, "w") as f:
                f.write(report)
            
            return {"message": f"Report generated at {output}", "report": report}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Report generation failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)