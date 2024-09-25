# Database Interaction
from sqlalchemy import Engine, update
# Update directory to import util
from util.sqlalchemy_tables import DatasetMetadata

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