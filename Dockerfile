FROM continuumio/anaconda3
RUN pip install luigi
ADD DataDownload.py /
ADD Data_PreProcessing_Loans.py /
ADD Data_PreProcessing_DeclinedLoans.py /
ADD Pipeline_Luigi.py /