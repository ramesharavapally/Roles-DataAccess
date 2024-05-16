from runreport import ErpReportService
import configparser
from readexceldata import get_user_roles_data_with_guid , get_dataccess_source_data , get_user_source_data
from userole import UserRoles
from dataccess import RoleDatAccess
from users import Users
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
            (df['USER_NAME'].str.upper().str.strip() == username.upper().strip()) & 
            (df['JOB_ROLE_NAME'].str.upper().str.strip() == rolename.upper().strip()) & 
            (df['SECURITY_CONTEXT'].str.upper().str.strip() == str(row['SecurityContext']).upper().strip()) & 
            (df['SECURITY_CONTEXT_VALUE'].str.upper().str.strip() == str(row['SecurityContextValue']).upper().strip())
        )
    ).any()

def __get_userrole_assignment_id(username : str , rolename: str , row:dict , df:pd.DataFrame) -> str:
    row = df[ (
            (df['USER_NAME'].str.upper().str.strip() == username.upper().strip()) & 
            (df['JOB_ROLE_NAME'].str.upper().str.strip() == rolename.upper().strip()) & 
            (df['SECURITY_CONTEXT'].str.upper().str.strip() == str(row['SecurityContext']).upper().strip()) & 
            (df['SECURITY_CONTEXT_VALUE'].str.upper().str.strip() == str(row['SecurityContextValue']).upper().strip())
        )].iloc[0]
    
    return int(row['USER_ROLE_DATA_ASSIGNMENT_ID']) , row['IS_DATA_ACCESS_ACTIVE']
            

def __filter_dataccess_data(json_data: list , df : pd.DataFrame) -> list:    
    create_filtered_data = []
    update_filter_data = []
        
    for record in json_data:        
        username = record['UserName']
        rolename = record['RoleName']
        
        create_data = []
        update_data = []
        
        
        # data = [row for row in record['dataAccess'] if not __is_combination_present(username=username , rolename= rolename , row=row , df=df)]
        # d = {
        #     "UserName" : username,
        #     "RoleName" : rolename,
        #     "dataAccess" : data
        # }
        # create_filtered_data.append(d)
                
        
        for row in record['dataAccess']:            
            if not __is_combination_present(username=username , rolename= rolename , row=row , df=df):
                create_data.append(row)
            else:
                row['USER_ROLE_DATA_ASSIGNMENT_ID'] , row['IS_DATA_ACTIVE']= __get_userrole_assignment_id(username=username , rolename= rolename , row=row , df=df)
                update_data.append(row)
        create_d = {
            "UserName" : username,
            "RoleName" : rolename,
            "dataAccess" : create_data
        }
        
        update_d = {
            "UserName" : username,
            "RoleName" : rolename,
            "dataAccess" : update_data
        }
        
        create_filtered_data.append(create_d)
        update_filter_data.append(update_d)
        
    return create_filtered_data , update_filter_data


def get_dataccess_data(df:pd.DataFrame) -> str:
    # erp_url , username , password , guid_report , roles_report = get_config_details()
    json_data = get_dataccess_source_data()  
    if json_data is None:
        return None              
    create_filter_data , update_filer_data = __filter_dataccess_data(json_data=json_data , df=df)    
    return json.dumps(create_filter_data , indent=4) , json.dumps(update_filer_data , indent= 4)


def save_latest_roles_report() -> None:
    erp_url , username , password , guid_report , roles_report = get_config_details()
    ErpReportService(erp_url,username,password).runrolesreport(roles_report , save_report_name= 'latest_user_roles.csv')


def main():
    erp_url , username , password , guid_report , roles_report = get_config_details()    
    
    setup_folders()
    cleanup_data()        
    
    print(f"*******Start of processing for user creation***************")
    
    users = get_user_source_data()        
    if users is None:
        print(f"there is not data to process for Users ")        
    else:
        Users(erp_url,username,password).create_users(users)
    
    print(f"*******End of processing for user creation***************\n")    
    
    status = ErpReportService(erp_url,username,password).runguidreport(guid_report , save_report_name='guid_report.xls')           
    df = ErpReportService(erp_url,username,password).runrolesreport(roles_report , save_report_name= 'temp.csv')
    if ('Success' != status) or (df is None):
        print('Error while creating the guid_report or roles report report')
        return
    
    print(f"*******Start of processing for user Roles creation***************")
    
    user_roles_data = get_user_roles_data_with_guid()        
    if user_roles_data is None:            
        print(f"there is not data to process for roles ")        
    else:
        UserRoles(erp_url,username,password).assign_roles_to_users(user_roles_data= user_roles_data)        
    
    print(f"*******End of processing for user Roles creation***************\n")
    
    print(f"*******Start of processing for Roles Data Access creation***************")
        
    create_dataccess_data , update_dataccess_data = get_dataccess_data(df=df) 
        
    if create_dataccess_data is None:
        print(f"there is not data to process for create dataccess sets ")        
    else:            
        RoleDatAccess(erp_url,username,password,'create').assign_dataccess_to_users(roles_dataaccess_data=create_dataccess_data)            
            
    if update_dataccess_data is None:
        print(f"there is no data to update data access data")
    else:
        RoleDatAccess(erp_url,username,password,'update').update_dataccess_to_users(roles_dataaccess_data=update_dataccess_data)                            
        
    save_latest_roles_report()
    print(f"*******End of processing for Roles Data Access creation***************\n")
            

if __name__ == '__main__':
    main()    
    print('Done')
    