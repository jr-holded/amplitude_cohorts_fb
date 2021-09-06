
from lib.integrations import BigQuery
import csv,re
import pandas as pd
from lib.types import Response
from datetime import datetime
from flask import make_response


def main(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <https://flask.palletsprojects.com/en/1.1.x/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'cohort_id' in request.args:
        return "shit"
    #data = parse_csv(request.args['cohort_id'])
    data = parse_csv('data/cohort.csv')
    if not data.success:
        return "shit"
    df = pd.DataFrame.from_records(data.data["rows"],columns=data.data["columns"])
    bq_client = BigQuery(project_id="holded-analytics-sb-jesus-01c3")
    dataset = "test"
    table_name =  str(datetime.timestamp(datetime.now()))[:10] 
    temp_table = bq_client.upload_dataframe(df,dataset,table_name)
    if not temp_table.success:
        return temp_table.message

    query = f"""
        SELECT
            ids.userid
            ,users.email
        FROM
            `holded-analytics-sb-jesus-01c3.{dataset}.{temp_table}` as ids
        LEFT JOIN
            `holded-analytics-prod-d185.source_fivetran_holded_operational_world.users` AS users
        ON 
            ids.userid = users._id 
        WHERE 
            users._fivetran_active = true
    """
    try:
        query_result = bq_client.query(query)
        df_result = query_result.to_dataframe()
    except Exception as e:
        return str(e)
        
    resp = make_response(df_result.to_csv())
    resp.headers["Content-Disposition"] = "attachment; filename=export.csv"
    resp.headers["Content-Type"] = "text/csv"
    return resp


def parse_csv(file_path:str)-> object:
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        data = {
            "columns":[],
            "rows":[]
        }
        for row in csv_reader:
            if line_count == 0: 
                data['columns'] = [re.sub('[^a-zA-Z0-9]+', '', _) for _ in row]
                line_count += 1
            else:
                data['rows'].append([re.sub('[^a-zA-Z0-9]+', '', _) for _ in row])
                line_count += 1
    return Response(True,"File loaded",data)
        