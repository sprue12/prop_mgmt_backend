from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery

app = FastAPI()

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "mgmt54500-project"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

def property_exists(property_id: int, bq: bigquery.Client) -> bool:
    query = f"""
        SELECT 1
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
        LIMIT 1
    """
    results = list(bq.query(query).result())
    return len(results) > 0


# ---------------------------------------------------------------------------
# Properties Endpoints (2)
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
    """

    results = bq.query(query).result()
    data = [dict(row) for row in results]

    if not data:
        raise HTTPException(status_code=404, detail="Property not found")

    return data[0]

#--------------------------------------------------------#
# Income Endpoints
#--------------------------------------------------------#

@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
    """
    results = bq.query(query).result()
    return [dict(row) for row in results]

from pydantic import BaseModel
import uuid

class Income(BaseModel):
    amount: float
    source: str

@app.post("/income/{property_id}")
def add_income(property_id: int, income: Income, bq: bigquery.Client = Depends(get_bq_client)):
    if not property_exists(property_id, bq):
        raise HTTPException(status_code=404, detail="Property not found")

    row = {
        "income_id": str(uuid.uuid4()),
        "property_id": property_id,
        "amount": income.amount,
        "source": income.source
    }

    table_id = f"{PROJECT_ID}.{DATASET}.income"
    errors = bq.insert_rows_json(table_id, [row])

    if errors:
        raise HTTPException(status_code=500, detail=str(errors))
    try:
        errors = bq.insert_rows_json(table_id, [row])
        if errors:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to insert income record: {errors}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )

    return {"message": "Income added", "data": row}

# -------------------------------------------------------------------------------
# Expense Endpoints
# -------------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = {property_id}
    """
    results = bq.query(query).result()
    return [dict(row) for row in results]

class Expense(BaseModel):
    amount: float
    category: str

@app.post("/expenses/{property_id}")
def add_expense(property_id: int, expense: Expense, bq: bigquery.Client = Depends(get_bq_client)):
    if not property_exists(property_id, bq):
        raise HTTPException(status_code=404, detail="Property not found")

    row = {
        "expense_id": str(uuid.uuid4()),
        "property_id": property_id,
        "amount": expense.amount,
        "category": expense.category
    }

    table_id = f"{PROJECT_ID}.{DATASET}.expenses"
    errors = bq.insert_rows_json(table_id, [row])

    if errors:
        raise HTTPException(status_code=500, detail=str(errors))
    try:
        errors = bq.insert_rows_json(table_id, [row])
        if errors:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to insert expense record: {errors}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )

    return {"message": "Expense added", "data": row}

# ----------------------------------------------------------------------------------
# 4 additional endpoints
# ----------------------------------------------------------------------------------

# 1) Create a Property
class Property(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: str
    monthly_rent: float


@app.post("/properties")
def create_property(property: Property, bq: bigquery.Client = Depends(get_bq_client)):
    try:
        id_query = f"""
            SELECT IFNULL(MAX(property_id), 0) + 1 AS next_id
            FROM `{PROJECT_ID}.{DATASET}.properties`
        """
        id_result = list(bq.query(id_query).result())
        next_id = id_result[0]["next_id"]

        row = {
            "property_id": next_id,
            **property.dict()
        }

        table_id = f"{PROJECT_ID}.{DATASET}.properties"
        errors = bq.insert_rows_json(table_id, [row])

        if errors:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create property: {errors}"
            )

        return {"message": "Property created", "data": row}

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )

# 2) Delete a Property
@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    if not property_exists(property_id, bq):
        raise HTTPException(status_code=404, detail="Property not found")

    query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
    """
    bq.query(query)

    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Delete failed: {str(e)}"
        )

    return {"message": "Property deleted"}

#3) Total Income Summary
@app.get("/summary/income/{property_id}")
def total_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT SUM(amount) AS total_income
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
    """
    results = bq.query(query).result()
    return [dict(row) for row in results]

#4) Net Profit
@app.get("/summary/profit/{property_id}")
def net_profit(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            IFNULL((SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id={property_id}),0)
            -
            IFNULL((SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id={property_id}),0)
        AS net_profit
    """
    results = bq.query(query).result()
    return [dict(row) for row in results]