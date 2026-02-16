# Database Interaction
from sqlalchemy import Engine, MetaData, select, update, insert
# Update directory to import util
from util.sqlalchemy_tables import DatasetMetadata, Users, DatasetAllCols, DatasetTempCols, DatasetTextCols

# Get all of the metadata of a dataset
def get_metadata(engine: Engine, meta: MetaData, table_name: str) -> dict:
    # Make query
    query = (
        select(DatasetMetadata)
            .where(DatasetMetadata.table_name == table_name)
    )
    output = None
    with engine.connect() as conn:
        for row in conn.execute(query):
            output = row
            break
        conn.commit()
        
    if output is None:
        raise Exception("Query failed")    
    
    # Convert row to dict using column names from the ORM model
    record = {}
    for col in DatasetMetadata.__table__.columns:
        record[col.name] = getattr(output, col.name, None)

    return record
        
# Get a user record by email
def get_user(engine: Engine, meta: MetaData, email: str) -> dict:
    # Make query
    query = (
        select(Users)
            .where(Users.email == email)
    )
    with engine.connect() as conn:
        for row in conn.execute(query):
            output = row
            break
        conn.commit()
        
    # Convert row to dict using column names from the ORM model
    record = {}
    for col in Users.__table__.columns:
        record[col.name] = getattr(output, col.name, None)

    return record

# Upload temporary columns for a dataset
def add_temp_cols(engine: Engine, table_name: str, cols: list[str]):
    data = list(map(lambda x: { "table_name": table_name, "col": x }, cols))
    
    # Make query
    query = (
        insert(DatasetTempCols)
            .values(data)
    )
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()

# Get all columns in a dataset
def get_cols(engine: Engine, table_name: str) -> list[str]:
    # Make query
    query = (
        select(DatasetAllCols.col)
            .where(DatasetAllCols.table_name == table_name)
    )
    
    # Get data from table
    cols = []
    with engine.connect() as conn:
        for row in conn.execute(query):
            cols.append(row[0])
            
    return cols

# Get the text columns in a dataset
def get_text_cols(engine: Engine, table_name: str) -> list[str]:
    # Make query
    query = (
        select(DatasetTextCols.col)
            .where(DatasetTextCols.table_name == table_name)
    )
    
    # Get data from table
    cols = []
    with engine.connect() as conn:
        for row in conn.execute(query):
            cols.append(row[0])
            
    return cols

# Update number of batches
def update_num_batches(engine: Engine, table_name: str, batch_num: int):
    # Make query
    query = (
        update(DatasetMetadata)
            .where(DatasetMetadata.table_name == table_name)
            .values({ "num_batches": batch_num })
    )
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()
        
# Update metadata with number of records
def set_num_records(engine: Engine, table_name: str, num_records: int) -> None:
    query = (
        update(DatasetMetadata)
            .where(DatasetMetadata.table_name == table_name)
            .values({
                "num_records": num_records
            })
    )
    
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit() 
