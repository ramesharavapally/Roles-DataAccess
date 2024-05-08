from runreport import ErpReportService
import configparser
from readexceldata import get_user_roles_data_with_guid , get_dataccess_source_data
from userole import UserRoles
from dataccess import RoleDatAccess
import pandas as pd
import json
import os
import shutil

CONFIG_FILE = r'..\config.ini'

def get_config_details() -> tuple :
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)    
    default_rec = config['DEFAULT']
    return default_rec['erp_url'] , default_rec['username'] , default_rec['password'] , default_rec['guid_report_path'] , default_rec['roles_report_path']

def setup_folders() -> None:
    guid_folder_path = r'..\data\guid'
    if os.path.exists(guid_folder_path):        
        shutil.rmtree(guid_folder_path)  
    os.makedirs(guid_folder_path)

def cleanup_data() -> None:
    folder_path = r'..\data'
    
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(folder_path, file)
            os.remove(file_path)
        
        
def __is_combination_present( username : str , rolename: str , row:dict , df:pd.DataFrame ) -> bool:                
     return (
        (
            (df['PERSON_NAME'].str.upper().str.strip() == username.upper().strip()) & 
            (df['JOB_ROLE_NAME'].str.upper().str.strip() == rolename.upper().strip()) & 
            (df['SECURITY_CONTEXT'].str.upper().str.strip() == str(row['SecurityContext']).upper().strip()) & 
            (df['SECURITY_CONTEXT_VALUE'].str.upper().str.strip() == str(row['SecurityContextValue']).upper().strip())
        )
    ).any()
    
    

def __filter_dataccess_data(json_data: list , df : pd.DataFrame) -> list:    
    filtered_data = []
        
    for record in json_data:        
        username = record['UserName']
        rolename = record['RoleName']
        data = [row for row in record['dataAccess'] if not __is_combination_present(username=username , rolename= rolename , row=row , df=df)]
        d = {
            "UserName" : username,
            "RoleName" : rolename,
            "dataAccess" : data
        }
        filtered_data.append(d)
    return filtered_data

def get_dataccess_data() -> str:
    erp_url , username , password , guid_report , roles_report = get_config_details()
    json_data = get_dataccess_source_data()    
    df = ErpReportService(erp_url,username,password).runrolesreport(roles_report , save_report_name= 'temp.csv')
    filter_data = __filter_dataccess_data(json_data=json_data , df=df)
    return json.dumps(filter_data , indent=4)


def save_latest_roles_report() -> None:
    erp_url , username , password , guid_report , roles_report = get_config_details()
    ErpReportService(erp_url,username,password).runrolesreport(roles_report , save_report_name= 'latest_user_roles.csv')


def main():
    erp_url , username , password , guid_report , roles_report = get_config_details()    
    setup_folders()
    cleanup_data()
    status = ErpReportService(erp_url,username,password).runguidreport(guid_report , save_report_name='guid_report.xls')    
    if 'Success' == status:
        user_roles_data = get_user_roles_data_with_guid()        
        UserRoles(erp_url,username,password).assign_roles_to_users(user_roles_data= user_roles_data)        
        dataccess_data = get_dataccess_data()        
        RoleDatAccess(erp_url,username,password).assign_dataccess_to_users(roles_dataaccess_data=dataccess_data)
        save_latest_roles_report()
    else:
        print(status)

if __name__ == '__main__':
    main()    
    print('Done')
    