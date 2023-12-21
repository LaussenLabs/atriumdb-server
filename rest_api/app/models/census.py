import sqlalchemy

metadata = sqlalchemy.MetaData()

Census = sqlalchemy.Table(
    "current_census",
    metadata,
    sqlalchemy.Column("admission_start", sqlalchemy.Integer),
    sqlalchemy.Column("unit_id", sqlalchemy.Integer),
    sqlalchemy.Column("unit_name", sqlalchemy.String),
    sqlalchemy.Column("bed_id", sqlalchemy.Integer),
    sqlalchemy.Column("bed_name", sqlalchemy.String),
    sqlalchemy.Column("patient_id", sqlalchemy.Integer),
    sqlalchemy.Column("mrn", sqlalchemy.Integer),
    sqlalchemy.Column("first_name", sqlalchemy.String),
    sqlalchemy.Column("middle_name", sqlalchemy.String),
    sqlalchemy.Column("last_name", sqlalchemy.String),
    sqlalchemy.Column("gender", sqlalchemy.String),
    sqlalchemy.Column("birth_date", sqlalchemy.Integer),
    sqlalchemy.Column("height", sqlalchemy.Float),
    sqlalchemy.Column("weight", sqlalchemy.Float),
)
