import pandas as pd
import os

data_dir = os.path.join(os.getcwd(), 'data')
output_dir = os.path.join(os.getcwd(), 'output')

class investDB:
    def __init__(self):
        self.df = None
        self.df_file = os.path.join(output_dir, 'investDB.pkl')
        self.cols = ['symbol', 'trade_date', 'trade_type', 'quantity', 'price']
        self.createDB()
    
    def createDB(self):
        self.df = pd.DataFrame()
        self.saveDB()

    def addToDB(self, csvfile):
        self.loadDB()
        new_df = pd.read_csv(csvfile, usecols=self.cols)
        new_df.loc[new_df['trade_type']=='sell', 'quantity'] = -new_df['quantity']
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        self.df.sort_values('trade_date')
        self.saveDB()

    def loadDB(self):
        self.df = pd.read_pickle(self.df_file)

    def saveDB(self):
        self.df.to_pickle(self.df_file)

print('Below files were found:')
filelist = []
for file in os.listdir(data_dir):
    print(file)
    filelist.append(os.path.join(data_dir, file))

db_obj = investDB()
print('Loading into DBs...')
for file in filelist:
    db_obj.addToDB(file)
    print('Added', file, 'to DB...')
    
print('Adding files complete')