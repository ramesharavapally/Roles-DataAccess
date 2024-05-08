import pandas as pd
import requests
import os
import base64
from io import BytesIO


class ErpReportService:
    
    def __init__(self , url: str , username : str , password : str) -> None:
        self.url = url
        self.username = username
        self.password = password
        self.headers = {'Content-Type': 'application/soap+xml'}
        
    
    def extract_report_bytes(self , response_text:str) -> str:        
        start_tag = "<ns2:reportBytes>"
        end_tag = "</ns2:reportBytes>"
        start_index =response_text.find(start_tag)
        end_index = response_text.find(end_tag)
        if start_index != -1 and end_index != -1:
            return response_text[start_index + len(start_tag):end_index].strip()
        else:
            return None
        
    def decode_base64_to_df( self , base64_data:str , save_report_name: str) -> pd.DataFrame:    
        # Decode base64 data
        decoded_data = base64.b64decode(base64_data)        
        bytes_io = BytesIO(decoded_data)
        df = pd.read_csv(bytes_io )        
        with open(r'..\data\{reportname}'.format(reportname=save_report_name), 'wb') as p:
            p.write(decoded_data)
        return df

    def save_guid_base64_to_report( self , base64_data:str , save_report_name: str) -> None:    
        # Decode base64 data
        decoded_data = base64.b64decode(base64_data)        
        bytes_io = BytesIO(decoded_data)        
        with open(r'..\data\guid\{reportname}'.format(reportname=save_report_name), 'wb') as p:
            p.write(decoded_data)        
        
    def runrolesreport(self, report_path: str , save_report_name : str) -> pd.DataFrame:
        
        soap_payload = f"""
        <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
           <soap:Header/>
           <soap:Body>
              <pub:runReport>
                 <pub:reportRequest>
                    <pub:attributeFormat>csv</pub:attributeFormat>
                    <pub:flattenXML>false</pub:flattenXML>                    
                    <pub:reportAbsolutePath>{report_path}</pub:reportAbsolutePath>
                    <pub:sizeOfDataChunkDownload>-1</pub:sizeOfDataChunkDownload>
                 </pub:reportRequest>
              </pub:runReport>
           </soap:Body>
        </soap:Envelope>
        """
        
        url = f'{self.url}/xmlpserver/services/ExternalReportWSSService?WSDL'
        response = requests.request(method='POST', url=url, data=soap_payload, headers=self.headers, auth=(self.username, self.password))        
        if response.status_code == 200:
            report_bytes = self.extract_report_bytes(str(response.text))            
            if report_bytes:
                df = self.decode_base64_to_df(report_bytes , save_report_name)                            
                return df
            else:
                return f"report_bytes are empty"
        else:
            return f"Error while invokeing report  {response.status_code} {response.text}"
    
    def runguidreport(self, report_path: str , save_report_name : str) -> str:
        
        soap_payload = f"""
        <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
           <soap:Header/>
           <soap:Body>
              <pub:runReport>
                 <pub:reportRequest>
                    <pub:attributeFormat>excel</pub:attributeFormat>
                    <pub:flattenXML>false</pub:flattenXML>  
                    <pub:parameterNameValues>
                       <pub:item>
                          <pub:name>rolename</pub:name>
                          <pub:values>
                             <pub:item></pub:item>
                          </pub:values>
                       </pub:item>
                       <pub:item>
                          <pub:name>username</pub:name>
                          <pub:values>
                             <pub:item></pub:item>
                          </pub:values>
                       </pub:item>
                    </pub:parameterNameValues>                  
                    <pub:reportAbsolutePath>{report_path}</pub:reportAbsolutePath>
                    <pub:sizeOfDataChunkDownload>-1</pub:sizeOfDataChunkDownload>
                 </pub:reportRequest>
              </pub:runReport>
           </soap:Body>
        </soap:Envelope>
        """
        
        url = f'{self.url}/xmlpserver/services/ExternalReportWSSService?WSDL'
        response = requests.request(method='POST', url=url, data=soap_payload, headers=self.headers, auth=(self.username, self.password))        
        if response.status_code == 200:            
            report_bytes = self.extract_report_bytes(str(response.text))            
            if report_bytes:
                self.save_guid_base64_to_report(report_bytes , save_report_name)                            
                return f"Success"
            else:
                return f"report_bytes are empty"
        else:
            return f"Error while invokeing report  {response.status_code} {response.text}"
        

