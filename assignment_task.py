import pandas as pd, os
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool


dirname = 'C:/Users/inumu/OneDrive/Desktop/docs/sravanth/S22 5110 Assignment 4/Multiprocessing'
filedir = os.listdir("{}".format(dirname))
file_names = ["{}/".format(dirname)+filename for filename in filedir if filename.endswith('csv')]
# file_names = file_names[:50] # Optional, Uncomment to proceed to limitted file names; Eg: 50 files




def apply_operation(df_chunks):
    first_3_count = pd.DataFrame([[df_chunks.__len__(), df_chunks.customerID.unique().__len__(), df_chunks.PaymentMethod.unique().__len__()]], columns=['Total rows accross all files', 'Customer Id Count(Unique)', 'Payment Method count(Unique)'])
    return first_3_count



def MultiReadingProcess(prs):
    processes = prs
    print('--->', file_names.__len__())
    chunk_size = int(file_names.__len__()/processes)

    chunks_lst = list()
    num_chunks = len(file_names) // chunk_size + 1

    for i in range(num_chunks-1):
        concat_data = pd.concat(map(pd.read_csv, file_names[i*chunk_size:(i+1)*chunk_size]))
        chunks_lst.append(concat_data)

    with ThreadPool(processes) as p:
        result = p.map(apply_operation, chunks_lst)

    df_reconstructed = pd.concat(result)
    multi_dataset = pd.DataFrame([[df_reconstructed['Total rows accross all files'].sum(), df_reconstructed['Customer Id Count(Unique)'].sum(), df_reconstructed['Payment Method count(Unique)'].sum()]], columns=['Total rows accross all files', 'Customer Id Count(Unique)', 'Payment Method count(Unique)'])
    print(multi_dataset)
    multi_dataset.to_csv('Multiprocess_count_result.csv', index=False)
    
    return 'MultiReadingPRocess Done!'


def Perfile_process(file_names):
    filelist = list()
    final_dataset = pd.DataFrame()

    for filename in file_names:
        chunk_dataset = pd.read_csv(filename)
        filelist.append([filename.rsplit('/', 1)[1], chunk_dataset[chunk_dataset.Churn == 'Yes']['customerID'].unique().__len__(), chunk_dataset[chunk_dataset.PaperlessBilling == 'Yes']['customerID'].unique().__len__()])
        final_dataset = final_dataset.append(chunk_dataset)

    Perfile_dataset = pd.DataFrame(filelist, columns = ['File Name', 'Per file, number of customers that have churned', 'Per file, number of customers that have paperless billing'])
    Perfile_dataset.to_csv('Per file, Churn_and_PaperlessBilling_Count_29_04_2022.csv', index=False)

    return 'Perfile process Completed!'



MultiReadingProcess(10)

Perfile_process(file_names)

    