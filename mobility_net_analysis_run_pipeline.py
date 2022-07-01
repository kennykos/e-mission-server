import logging
from logging import config
import emission.core.get_database as edb
import emission.pipeline.intake_stage as epi
users = list(edb.get_uuid_db().find())
uuid_list = []
for user in users:
    uuid_list.append(user['uuid'])
epi.run_intake_pipeline(str(len(uuid_list)), uuid_list)
