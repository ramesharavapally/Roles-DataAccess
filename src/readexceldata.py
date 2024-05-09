import openpyxl
import json
import os
from datetime import datetime
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


def is_file_open(file_path) -> bool:
    try:
        with open(file_path, 'a') as f:
            pass
        return False
    except PermissionError:
        return True    
    
def is_sheet_empty(file_path, sheet_name):
    df = pd.read_excel(file_path, sheet_name)    
    return df.empty    

def __read_sheet_as_strings(file_path, sheet_name) -> list:
    # Load the workbook
    workbook = openpyxl.load_workbook(file_path, data_only=True)

    # Extract data from the specified sheet
    sheet = workbook[sheet_name]
    data_sheet = []

    for row in sheet.iter_rows(min_row=2, values_only=True):
        row_data = []
        for cell in row:
            if cell is None:
                row_data.append("")  # Treat None as an empty string
            else:
                if isinstance(cell , datetime):
                    cell = str(datetime.strftime(cell , '%m/%d/%y'))
                row_data.append(str(cell).strip())  # Convert cell value to string
        data_sheet.append(row_data)

    return data_sheet

def __user_roles_excel_to_json(file_path) -> list:
        
    if is_file_open(file_path):
        raise Exception(f"Error: File '{file_path}' is already open. Please close the file and try again.")                
    if is_sheet_empty(file_path=file_path , sheet_name='Roles'):
        return None
    sheet = __read_sheet_as_strings(file_path= file_path , sheet_name='Roles')
    
    # Define an empty list to store the data
    data = []
    
    # Iterate over rows in the Excel sheet, starting from the second row to skip the header    
    for row in sheet:
        # Create a dictionary to store each row of data
        row = list(row)
        if all(rec == '' for rec in row):
            continue
        for i in range(len(row)):
            if row[i] is None:
                row[i] = ""
        row_data = {
            'UserName': row[0],
            'RoleName':row[1],
            'Operation': row[2]            
        }
        # Append the row dictionary to the data list
        data.append(row_data)        
    return data



def __user_roles_source_data(folder_path : str) -> object:                
    # List to store JSON data from all files
    json_data_array = []

    # Iterate over files in the folder
    for filename in os.listdir(folder_path):        
        if filename.endswith('.xlsx'):
            file_path = os.path.join(folder_path, filename)
            json_data = __user_roles_excel_to_json(file_path)
            if json_data is None:
                continue
            json_data_array.append(json_data)
    
    output = []    
    if len(json_data_array) == 0 : 
        return None
    for record in json_data_array:
        output.extend(record)
    df = pd.DataFrame(output)
    grouped_df = df.groupby('UserName').apply(lambda x: x[['RoleName', 'Operation']].to_dict('records')).reset_index(name='Roles')        
    transformed_json = grouped_df.to_json(orient='records' , indent=4)
    if transformed_json is None:
        return None
    else:        
        return json.loads(transformed_json)
    

def __read_guid_meta_data( guid_folder_path : str) -> list[pd.DataFrame] :
    users_dfs = []
    roles_dfs = []
    for file in os.listdir(guid_folder_path):
        if file.endswith('.xlsx') or file.endswith('.xls'): 
            file_path = os.path.join(guid_folder_path, file)
            user_df = pd.read_excel(file_path , sheet_name='USER GUID')
            role_df = pd.read_excel(file_path , sheet_name='ROLE GUID')
            users_dfs.append(user_df)
            roles_dfs.append(role_df)    
    users_df = pd.concat(users_dfs , ignore_index=True)
    roles_df = pd.concat(roles_dfs , ignore_index= True)
    return users_df , roles_df

def __replace_with_ids(data, user_df , role_df) -> list:
    updated_data = []    
    for item in data:   
        user_role_dict = {}             
        user_role_dict['UserName'] = item['UserName']        
        if user_df['USERNAME'].str.contains(item['UserName']).any():            
            user_id = user_df.loc[user_df['USERNAME'] == item['UserName'], 'USER GUID'].values[0]            
            user_role_dict['UserID'] = user_id
            roles = []
            for role in item['Roles']:
                role_dict = {}                        
                role_dict['RoleName'] = role['RoleName']
                role_dict['Operation'] = role['Operation']
                if role_df['ROLENAME'].str.contains(role['RoleName']).any():
                    role_guid = role_df.loc[role_df['ROLENAME'] == role['RoleName'], 'ROLE GUID'].values[0]
                    role_dict['RoleID'] = role_guid
                else:
                    role_dict['RoleID'] = None
                roles.append(role_dict)
            
            user_role_dict['roles'] = roles
        else:
            user_role_dict['UserID'] = None
        updated_data.append(user_role_dict)
                
    return updated_data    


def get_user_roles_data_with_guid() -> str:
    guid_folder_path = r'..\data\guid'
    roles_folder_path = r'..\data\roles'
    source_json_data = __user_roles_source_data(roles_folder_path)        
    if source_json_data is None:        
        return None
    user_df , role_df = __read_guid_meta_data(guid_folder_path=guid_folder_path)
    data = __replace_with_ids(data= source_json_data , user_df= user_df , role_df= role_df)
    return json.dumps(data , indent=4)


##
## Start for get the dat for data acccess set
##



def __roles_dataccess_excel_to_json(file_path) -> list:        
    if is_file_open(file_path):
        raise Exception(f"Error: File '{file_path}' is already open. Please close the file and try again.")            
    if is_sheet_empty(file_path= file_path , sheet_name='DataAccess'):
        return None
    sheet = __read_sheet_as_strings(file_path= file_path , sheet_name='DataAccess')
    
    # Define an empty list to store the data
    data = []
    
    # Iterate over rows in the Excel sheet, starting from the second row to skip the header    
    for row in sheet:
        # Create a dictionary to store each row of data
        row = list(row)
        if all(rec == '' for rec in row):
            continue
        for i in range(len(row)):
            if row[i] is None:
                row[i] = ""
        row_data = {
            'UserName': row[0],
            'RoleName':row[1],
            'SecurityContext': row[2] ,
            "SecurityContextValue" : row[3]
            
        }
        # Append the row dictionary to the data list
        data.append(row_data)        
    return data


def get_dataccess_source_data() -> object:                
    # List to store JSON data from all files
    json_data_array = []
    folder_path = r'..\data\roles'

    # Iterate over files in the folder
    for filename in os.listdir(folder_path):        
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            file_path = os.path.join(folder_path, filename)
            json_data = __roles_dataccess_excel_to_json(file_path)
            if json_data is None:
                continue
            json_data_array.append(json_data)
    
    output = []
    if len(json_data_array) == 0:
        return None
    for record in json_data_array:
        output.extend(record)
    df = pd.DataFrame(output)
    grouped_df = df.groupby(['UserName','RoleName']).apply(lambda x: x[['SecurityContext' , 'SecurityContextValue']].to_dict('records')).reset_index(name='dataAccess')        
    transformed_json = grouped_df.to_json(orient='records' , indent=4)
    if transformed_json is None:
        return None
    else:                        
        return json.loads(transformed_json)
    


        

# print(get_user_roles_data_with_guid())