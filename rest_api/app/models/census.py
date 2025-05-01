#
# AtriumDB is a timeseries database software designed to best handle the unique
# features and challenges that arise from clinical waveform data.
#
# Copyright (c) 2025 The Hospital for Sick Children.
#
# This file is part of AtriumDB 
# (see atriumdb.io).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
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
