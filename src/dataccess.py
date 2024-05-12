import pandas as pd
import os
import json
import requests
import configparser
import traceback
from logger import Logger
import datetime


class RoleDatAccess:
    
    def __init__(self , erp_url :str , user_name : str , password:str ) -> None:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.erp_url = erp_url
        self.user_name = user_name
        self.password = password                
        self.logger = None
        self.logger = Logger(log_file=r'..\logs\dataccess_{timestamp}.log'.format(timestamp=timestamp) , logger_name='DataAccessLogger')
        self.headers = {
                    'Content-Type': 'application/vnd.oracle.adf.batch+json'                    
                        }
        
    def preparePayload(self , data : dict) -> object:
        
        payloads = []
        for record in data['dataAccess']:
            payload  = {                            
                            "SecurityContext": record["SecurityContext"],
                            "SecurityContextValue": record["SecurityContextValue"],
                            "RoleNameCr": data["RoleName"],
                            "UserName": data["UserName"]
                        }
            payloads.append(payload)
        result_payload = {
            "parts":[
                {
                    "id" : f"part{i+1}",
                    "path": "/dataSecurities",
                    "operation": "create",
                    "payload": payloads[i]
                } for i in range(len(payloads))
            ]
        }
        return result_payload
        
    
    def assign_dataccess_to_users(self , roles_dataaccess_data:str) -> None:
        roles_dataccess = json.loads(roles_dataaccess_data)
        api_url = '/fscmRestApi/resources/11.13.18.05'    
        url = f"{self.erp_url}{api_url}"
        
        for role in roles_dataccess:      
            print(f"""processing for the user "{role['UserName']}" and role "{role['RoleName']}" with "{[ (r['SecurityContext'] , r['SecurityContextValue']) for r in role['dataAccess'] ]}" """)
            self.logger.info(f"""processing for the user "{role['UserName']}" and role "{role['RoleName']}" with "{[ (r['SecurityContext'] , r['SecurityContextValue']) for r in role['dataAccess'] ]}" """)                                                  
            payload = json.dumps(self.preparePayload(role), indent=4)                           
            # self.logger.debug(payload)
            response = requests.post(url=url , auth =(self.user_name , self.password),data =  payload , headers = self.headers)                                    
            if response.status_code != 200:
                self.logger.error(f"Error with status : {response.status_code} {response.text}\n\n")                     
            else:
                self.logger.info(f"Success with status : {response.status_code}\n\n")                                             
                                                