import json
import requests
import csv
from lib.types import Response

class Amplitude:

    def __init__(self, api_key: str, secret_key:str=None) -> None:
        self.api_key = api_key
        if secret_key:
            self.secret_key = secret_key


    def __format_events(self, raw_events: list) -> str:
        events = {"api_key" : self.api_key, "events": raw_events}
        return json.dumps(events)

    def __format_group_identify(self, raw_events: list) -> dict:
        events = {"api_key": self.api_key, "identification": json.dumps(raw_events)}
        return events
    
    def track(self, events: list) -> Response:
        events = self.__format_events(events)
        url = "https://api2.amplitude.com/batch"
        headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*'
        }       
        res = requests.post(url, params={}, headers = headers, data=events)
        if res.status_code == 200:
            return Response(True, res.content)
        else:
            return  Response(False, res.content)
    
    def group_identify(self, raw_events: list) -> Response: 
        events = self.__format_group_identify(raw_events)
        url = "https://api2.amplitude.com/groupidentify"
        headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*'
        }       
        res = requests.post(url, params={}, headers = headers, data=events)
        if res.status_code == 200:
            return Response(True, res.content)
        else:
            return  Response(False, res.content)
    
    def get_cohort(self,cohort_id:str)-> Response:
        url = f"https://amplitude.com/api/5/cohorts/request/{cohort_id}?props=0"
        res = requests.post(url, params={}, auth=(self.api_key, self.secret_key))
        if res.status_code == 200:
            return Response(True,"Everythong OK" ,res.json())
        else:
            return  Response(False,f"Error: {res.text}")
    
    def get_cohort_status(self, request_id:str)-> Response:

        url = f"https://amplitude.com/api/5/cohorts/request/request-status/{request_id}"
        res = requests.post(url, params={}, auth=(self.api_key, self.secret_key))
        if res.status_code == 200:
            return Response(True,"Everythong OK" ,res.json())
        else:
            return  Response(False,f"Not ready: {res.text}")

    def download_cohort_(self, request_id:str)-> Response:
       
        url = f"https://amplitude.com/api/5/cohorts/request/{request_id}/file"
        res = requests.post(url, params={}, auth=(self.api_key, self.secret_key))
        with requests.get(url, stream=True,auth=(self.api_key, self.secret_key)) as r:
            lines = (line.decode('utf-8') for line in r.iter_lines())
            response = []
            for row in csv.reader(lines):
                response.append(row)
            return Response(True,"Everythong OK" ,res.json())




        
    