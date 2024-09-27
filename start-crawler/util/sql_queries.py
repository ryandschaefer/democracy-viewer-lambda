# Database Interaction
from sqlalchemy import Engine, MetaData, select, update
# Update directory to import util
from util.sqlalchemy_tables import DatasetMetadata, Users

# Update metadata that upload is done
def complete_upload(engine: Engine, table_name: str) -> None:
    query = (
        update(DatasetMetadata)
            .where(DatasetMetadata.table_name == table_name)
            .values({
                DatasetMetadata.uploaded: True
            })
    )
    
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()
        
# Update metadata that processing is done
def complete_processing(engine: Engine, table_name: str, processing_type: str) -> None:
    query = (
        update(DatasetMetadata)
            .where(DatasetMetadata.table_name == table_name)
            .values({
                f"{ processing_type }_done": True
            })
    )
    
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()
        
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
    
    # Give column names as keys
    record = {}
    for i, col in enumerate(meta.tables[DatasetMetadata.__tablename__].columns.keys()):
        if i < len(output):
            record[col] = output[i]
        
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
        
    # Give column names as keys
    record = {}
    for i, col in enumerate(meta.tables[Users.__tablename__].columns.keys()):
        record[col] = output[i]
        
    return record