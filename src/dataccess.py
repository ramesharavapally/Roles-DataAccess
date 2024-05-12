import pandas as pd
import os
import json
import requests
import configparser
import traceback
from logger import Logger
import datetime


class RoleDatAccess:
    
    def __init__(self , erp_url :str , user_name : str , password:str  , action:str) -> None:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.erp_url = erp_url
        self.user_name = user_name
        self.password = password                        
        self.logger = Logger(log_file=r'..\logs\{action}_dataccess_{timestamp}.log'.format(action=action , timestamp=timestamp) , logger_name=f"DataAccess_{action}_{timestamp}")
        self.headers = {
                    'Content-Type': 'application/vnd.oracle.adf.batch+json'                    
                        }
        
    def preparePayload(self , data : dict) -> object:
        
        payloads = []
        for record in data['dataAccess']:
            payload  = {                            
                            "UserName": data["UserName"],
                            "RoleNameCr": data["RoleName"],
                            "SecurityContext": record["SecurityContext"],
                            "SecurityContextValue": record["SecurityContextValue"]                                                        
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

    def prepareUpdatePayload(self , data : dict) -> object:
        payloads = []
        
        for i , record in enumerate(data['dataAccess']):
            payload = {
                "id" : f"part{i+1}",
                "path" : f"/dataSecurities/{record['USER_ROLE_DATA_ASSIGNMENT_ID']}",
                "operation" : "update",
                "payload" : {
                    "ActiveFlag" : True
                }
            }
            payloads.append(payload)
        
        result_payload = {
            "parts":payloads
        }
        return result_payload
    
    
    def update_dataccess_to_users(self , roles_dataaccess_data : str) -> None:
        roles_dataccess = json.loads(roles_dataaccess_data)
        api_url = '/fscmRestApi/resources/11.13.18.05'    
        url = f"{self.erp_url}{api_url}"
        
        self.logger.info(f""" Start processing for roles update data access  \n""")                
                
        
        for role in roles_dataccess:           
            
            if len(role['dataAccess']) == 0:                
                continue
            
            print(f"""processing update dataccess for the user "{role['UserName']}" and role "{role['RoleName']}" with "{[ (r['SecurityContext'] , r['SecurityContextValue']) for r in role['dataAccess'] ]}" """)
            self.logger.info(f"""processing update dataccess for the user "{role['UserName']}" and role "{role['RoleName']}" with "{[ (r['SecurityContext'] , r['SecurityContextValue']) for r in role['dataAccess'] ]}" """)                                                  
            
            payload = json.dumps(self.prepareUpdatePayload(role), indent=4)  
            # self.logger.debug(payload)
            response = requests.post(url=url , auth =(self.user_name , self.password),data =  payload , headers = self.headers)                                    
            if response.status_code != 200:
                self.logger.error(f"Error with status : {response.status_code} {response.text}\n\n")                     
            else:
                self.logger.info(f"Success with status : {response.status_code}\n\n")                                                                                  
        
        self.logger.info(f"""End of processing for roles update data access""")                
            
    
    def assign_dataccess_to_users(self , roles_dataaccess_data:str) -> None:
        roles_dataccess = json.loads(roles_dataaccess_data)
        api_url = '/fscmRestApi/resources/11.13.18.05'    
        url = f"{self.erp_url}{api_url}"
        
        self.logger.info(f""" Start processing for roles create data access  \n""")                
        
        for role in roles_dataccess:                 
            
            #Check if any new data access to process for user
            if len(role['dataAccess']) == 0:
                self.logger.error(f"""there is no create data access for for for the user "{role['UserName']}" and role "{role['RoleName']}" \n""")
                print(f"""there is no create data access for for for the user "{role['UserName']}" and role "{role['RoleName']}" """)
                continue
            
            print(f"""processing create dataccess for the user "{role['UserName']}" and role "{role['RoleName']}" with "{[ (r['SecurityContext'] , r['SecurityContextValue']) for r in role['dataAccess'] ]}" """)
            self.logger.info(f"""processing create dataccess for the user "{role['UserName']}" and role "{role['RoleName']}" with "{[ (r['SecurityContext'] , r['SecurityContextValue']) for r in role['dataAccess'] ]}" """)                                                  
            payload = json.dumps(self.preparePayload(role), indent=4)                           
            # self.logger.debug(payload)
            response = requests.post(url=url , auth =(self.user_name , self.password),data =  payload , headers = self.headers)                                    
            if response.status_code != 200:
                self.logger.error(f"Error with status : {response.status_code} {response.text}\n\n")                     
            else:
                self.logger.info(f"Success with status : {response.status_code}\n\n")     
        
        self.logger.info(f"""End of processing for roles create data access""")                                                        
                                                