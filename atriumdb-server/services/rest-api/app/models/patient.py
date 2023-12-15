import sqlalchemy

metadata = sqlalchemy.MetaData()

Patient = sqlalchemy.Table(
    "patient",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("mrn", sqlalchemy.Integer),
    sqlalchemy.Column("gender", sqlalchemy.String),
    sqlalchemy.Column("dob", sqlalchemy.Integer),
    sqlalchemy.Column("first_name", sqlalchemy.String),
    sqlalchemy.Column("middle_name", sqlalchemy.String),
    sqlalchemy.Column("last_name", sqlalchemy.String),
    sqlalchemy.Column("first_seen", sqlalchemy.Integer),
    sqlalchemy.Column("last_updated", sqlalchemy.Integer),
    sqlalchemy.Column("source_id", sqlalchemy.Integer),
    sqlalchemy.Column("height", sqlalchemy.Float),
    sqlalchemy.Column("weight", sqlalchemy.Float),
)
