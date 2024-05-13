import pandas as pd
import json
import requests
import configparser
import traceback
from logger import Logger
import datetime


class Users:
    
    def __init__(self , erp_url :str , user_name : str , password:str ) -> None:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.erp_url = erp_url
        self.user_name = user_name
        self.password = password        
        self.logger = Logger(log_file=r'..\logs\users_{timestamp}.log'.format(timestamp=timestamp) , logger_name='UsersLogger')
        self.headers = {
                    'Content-type':'application/json', 
                    'Accept':'application/json'
                        }
    
    def get_report_data(self) -> pd.DataFrame:
        return pd.read_csv(r'..\data\temp.csv')
    
    def __is_combination_present(self , username : str) -> bool:
        api_url = f"""/hcmRestApi/scim/Users?filter=userName eq "{username}" """
        api_url = api_url.strip()
        url = f"{self.erp_url}{api_url}"
        try:
            response = requests.get(url=url , auth =(self.user_name , self.password), headers = self.headers)             
            if response.status_code == 200:
                resources = json.loads(response.text)                
                # resources = resources['Resources']
                if len(resources['Resources']) > 0:
                    return True
                else:
                    return False
            else:
                return False
        except Exception as e:
            return False
    
    # def __is_combination_present(self , username : str , df:pd.DataFrame) -> bool :
    #     # return(
    #     #         df['USER_NAME'].str.upper().str.strip() == username.upper().strip()
    #     # ).any()
    #     api_url = f"""/hcmRestApi/scim/Users?filter=userName eq "{username}""""
    #     url = f"{self.erp_url}{api_url}"
    #     try:
    #         response = requests.get(url=url , auth =(self.user_name , self.password), headers = self.headers)
            
    #         if response.status_code == 200:
    #             return True
    #         else:
            
    #     except Exception as e:
    #         return False
        

    def create_users(self , users_data:str) -> None:        
        users_data = json.loads(users_data)
        api_url = '/hcmRestApi/scim/Users'    
        df = self.get_report_data()
        url = f"{self.erp_url}{api_url}"
        
        self.logger.info(f""" Start processing for Users create data   \n""")                
        
        for user in users_data:      
            try:      
                print(f"processing for the user {user['userName']}")            
                self.logger.info(f"processing for the user {user['userName']}")        
                if self.__is_combination_present(user['userName']):
                    self.logger.error(f""" user "{user['userName']}" already present """)
                    continue
                payload = f"""
                            {{
                                "schemas":[
                                    "urn:scim:schemas:core:2.0:User"
                                ],
                                "name": {{
                                    "familyName": "{user['lastName']}",
                                    "givenName": "{user['firstName']}"
                                }},
                                "active": true,
                                "userName": "{user['userName']}",
                                "emails" : [
                                    {{
                                        "primary": true,
                                        "value": "{user['email']}",
                                        "type": "W"
                                    }}
                                ],
                                "displayName": "{user['userName']}",
                                "password": "Welcome@1"
                            }}
                            """     
                # self.logger.debug(payload)       
                response = requests.post(url=url , auth =(self.user_name , self.password),data =  payload , headers = self.headers)
                self.logger.info(f"User: {user['userName']}  with status : {response.status_code} and user {'created' if response.status_code==201 else 'creation failed'}")            
            except Exception as e:
                    tb_info = traceback.format_exc()                            
                    self.logger.error(f"""Error while processing user "{user['userName']}" """)
                    self.logger.error(f"An unexpected error occurred: \n{e} \n {tb_info}\n\n")                                             
            
        self.logger.info(f""" End processing for Users create data   \n""")                            


