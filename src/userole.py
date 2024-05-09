import pandas as pd
import json
import requests
import configparser
import traceback
from logger import Logger
import datetime





class UserRoles:
    
    def __init__(self , erp_url :str , user_name : str , password:str ) -> None:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.erp_url = erp_url
        self.user_name = user_name
        self.password = password        
        self.logger = Logger(log_file=r'..\logs\roles_{timestamp}.log'.format(timestamp=timestamp) , logger_name='RolesLogger')
        self.headers = {
                    'Content-type':'application/json', 
                    'Accept':'application/json'
                        }

    def assign_roles_to_users(self , user_roles_data:str) -> None:        
        user_roles = json.loads(user_roles_data)
        api_url = '/hcmRestApi/scim/Roles/'    
        
        for user in user_roles:            
            print(f"processing for the user {user['UserName']}")            
            self.logger.info(f"processing for the user {user['UserName']}")        
            if user['UserID'] is None or user['UserID'] == '':
                self.logger.error(f"Invalid user {user['UserName']}")
                continue
            for role in user['roles']:
                try:    
                    if role['RoleID'] is None or role['RoleID'] == '':
                        self.logger.error(f"Invalid Role {role['RoleName']}")
                        continue
                    url = f"{self.erp_url}{api_url}{role['RoleID']}"
                    payload = payload = f""" {{                                     
                                                "members": [
                                                    {{
                                                        "value": "{user["UserID"]}", 
                                                        "operation": "{role['Operation']}"
                                                    }}
                                                ]  
                                                }}                                      
                                            """                                            
                    response = requests.patch(url=url , auth =(self.user_name , self.password),data =  payload , headers = self.headers)                        
                    self.logger.info(f"role: {role['RoleName']} , operation : {role['Operation']} , with status : {response.status_code}")                                
                except Exception as e:
                    tb_info = traceback.format_exc()                            
                    self.logger.error(f"Error while processing role {role['RoleName']}")
                    self.logger.error(f"An unexpected error occurred: \n{e} \n {tb_info}\n\n")                                             
            


