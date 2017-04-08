import luigi
import DataDownload
import Data_PreProcessing_Loans
import Data_PreProcessing_DeclinedLoans
from boto.s3.key import Key
from boto.s3.connection import S3Connection
import time
import datetime

class DataDownloadcls(luigi.Task):
    acckey = luigi.Parameter()
    accsec = luigi.Parameter()
    
    def requires(self):
        return []
    
    def output(self):
        return []
    
    def run(self):
        DataDownload.cleanup_dir() 
        DataDownload.loan_data_download()
        DataDownload.loan_data_unzip_extract()
        DataDownload.reject_loan_data_download()
        print('inside d')
        DataDownload.reject_loan_data_unzip_extract()
        print('Data download completed!!!!!!!!!!!!!!!')
        print('Starting Preprocessing of accepted loans!!!!!!!!!')
        Data_PreProcessing_Loans.loan_preprocess_magic()
        print('Preprocessing of accepted loans complete!!!!!!!!!')
        print('Starting Preprocessing of rejected loans !!!!!!!!!')
        Data_PreProcessing_DeclinedLoans.declinedLoan_preprocess_magic()
        print('Preprocessing of rejected loans complete!!!!!!!!!')
        print('Uploading to S3 started!!!!!!!!!')
        access_key = self.acckey 
        access_secret = self.accsec
        conn = S3Connection(access_key, access_secret)

        #Connecting to the bucket
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts)
        bucket_name = access_key.lower()+str(st).replace(" ", "").replace("-", "").replace(":","").replace(".","")
        bucket = conn.create_bucket(bucket_name)
        #bucket_name = "assignment2team9apr7"
        #bucket = conn.get_bucket(bucket_name)

        #Setting up the keys
        k1 = Key(bucket)
        k1.key = "cleanedloan"
        k1.set_contents_from_filename("Cleaned_all_data.csv")
        print('CLeaned loan data uploaded to S3')
        
        k2 = Key(bucket)
        k2.key = "cleaneddeclinedloan"
        k2.set_contents_from_filename("Cleaned_all_data_declined.csv")
        print('CLeaned rejected loan data uploaded to S3')
        print('If you are seeing this message it means that the efforts of Team9 has paid off and program has ended successfully.')

if __name__ == '__main__':
    luigi.run()
