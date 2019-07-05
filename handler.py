import json
import os
from data_prep import DataPrep

def run_data_prep(event, context):
#def run_data_prep():
    #inp = '{"reference": "4990012", "period": "201211", "survey": "066", "instance": "instanceId"}'
    #event = json.loads(inp)

    print(event)
    dataprep = DataPrep(event)
    records = dataprep.get_qcode_resp_from_db()
    dataprep.construct_response(records)
    print("Attempting to invoke Wrangler Lambda with the json string: " + str(event))
    dataprep.send_data_to_wrangler()

#run_data_prep()