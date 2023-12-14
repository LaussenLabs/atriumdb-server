import sqlalchemy

metadata = sqlalchemy.MetaData()

Encounter = sqlalchemy.Table(
    "encounter",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("patient_id", sqlalchemy.Integer),
    sqlalchemy.Column("bed_id", sqlalchemy.Integer),
    sqlalchemy.Column("start_time", sqlalchemy.Integer),
    sqlalchemy.Column("end_time", sqlalchemy.Integer),
    sqlalchemy.Column("source_id", sqlalchemy.Integer),
    sqlalchemy.Column("visit_number", sqlalchemy.String),
    sqlalchemy.Column("last_updated", sqlalchemy.Integer),
    sqlalchemy.Column("source_id", sqlalchemy.Integer),
)
