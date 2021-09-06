import logging
from typing import List, Sequence, Union
from google.cloud import bigquery
from lib.types import Response
import pandas as pd


class BigQuery():

	def __init__(self, project_id: str = None) -> None:
		logging.info("Creating BigQuery Client")
		if project_id:
			self.client = bigquery.Client(project=project_id)
		else:
			self.client = bigquery.Client()

	def query(self, query: str) -> Sequence:
		query_job = self.client.query(query)  # API request
		rows = query_job.result()
		return rows

	def insert(self, table_id: str, data: Union[dict, List[dict]]) -> Response:

		large_batch = False
		batch_size = 1000

		if len(data) == 0:
			return Response(False, "[BigQuery][insert] > No rows to add")
		elif not isinstance(data, list):
			data = [data]

		table = self.client.get_table(table_id)

		if len(data) > batch_size:
			logging.info(f"[BigQuery][insert] > Extra large batch ", len(data))
			errors = self.client.insert_rows(table, data[:batch_size])
			large_batch = True
		else:
			errors = self.client.insert_rows(table, data)

		if not errors:
			if large_batch:
				del data[:batch_size]
				logging.info(f"[BigQuery][insert] > {batch_size} new rows have been added to {table_id}.")
				logging.info(f"[BigQuery][insert] > Calling again function [insert]")
				return self.insert(table_id, data)

			return Response(True, f"[BigQuery][insert] > {len(data)} new rows have been added to {table_id}.")
		else:
			for err in errors:
				logging.error(err)
			return Response(False, str(errors))
	
	def upload_dataframe(self,dataframe:pd.DataFrame,dataset:str,table:str) -> Response:
		
		try:
			table = self.client.dataset(dataset).table(table)

			job_config = bigquery.LoadJobConfig()
			job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
			job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
			job = self.client.load_table_from_dataframe(dataframe, table, job_config=job_config)
			job.result()
			assert job.state == "DONE"
			return Response(True,"Data uploaded to BigQuery")
		except Exception as e:
			return Response(False,f"Error: {e}")

	def delete_table(self, table_id:str):
		try:
			res = self.client.delete_table(table_id, not_found_ok=False)
			return Response(True,"Table deleted")
		except Exception as e:
			return Response(False,f"Error: {e}")