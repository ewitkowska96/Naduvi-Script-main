import os
from datetime import date
from currency_converter import CurrencyConverter
import pandas as pd

#Create dataframes for further use
PATH = os.path.dirname(os.path.abspath(__file__))
df1 = pd.read_excel('Rpt.xls')
df2 = pd.read_excel('Rpt-wagi.xls')
today = str(date.today())
c = CurrencyConverter()

df1.sort_values(by=['ORDER', 'LP'], inplace=True)
df2.sort_values(by=['PO', 'lp'], inplace=True)

for file_name in os.listdir(PATH):
    if file_name.endswith('.csv'):
        source = PATH + '\\' + file_name
        name = 'import' + ' ' + today
        destination = name + '.csv'
        os.rename(source, destination)
df3 = pd.read_csv(destination, sep=',', encoding='utf-8')

# def search():
#     ser = pd.Series(df1['PO'], dtype='str')
#     list = []
#     a = ser.drop_duplicates()
#     b = a.str[15:].to_list()
#     print(type(ser[0]))
#     return b

#Get currency rate for insurance of the package
def currency_convertor(col):
    amount = col['cena']
    new_amount = c.convert(amount, 'EUR', 'PLN')
    return new_amount

#Filter naduvi import file, append sorted content to new dataframe
def case_wuunder():

    search = ['26360-01', '26342-01', '26268-01', '26252-01' ,'26220-01' ,'26160-01' ,'26063-01' ,'26059-01', '25922-01', '25868-01', '25540-01', '25768-01', '25768-01', '25503-01']
    new_cols = ['numCDEClient', 'nom_dest', 'ad1_dest', 'cp_dest', 'ville_dest', 'pays_dest', 'telfixe_dest', 'email_dest', 'nbcolis_unit', 'vol_unit', 'pds_unit', 'ref']

    #Fix references
    temp = df3.rename(columns={'SKU':'ref'}, inplace=True)
    temp = df3.replace('BESO-', '', inplace=True, regex=True)

    trimmed_df1 = df1.loc[:, ['REF', 'LP', 'ORDER']].sort_values(by=['REF'])
    trimmed_df2 = df2.loc[:, ['ref', 'pack', 'vol', 'weight', 'cena']].sort_values(by=['ref'])

    temp = df3[df3['Order reference'].isin(search)].sort_values(by=['ref'])

    temp = temp.join(trimmed_df1.set_index('REF'), on='ref', rsuffix='REF')
    temp = temp.join(trimmed_df2.set_index('ref'), on='ref', rsuffix='ref')

    #Reindex new dataframe
    final = pd.DataFrame([])
    final = temp.loc[:, :]
    final = final[['Order reference', 'Full name', 'Address line 1', 'Postcode', 'City', 'Country', 'Phone', 'Email', 'pack', 'vol', 'weight', 'ref']]
    final.set_axis(new_cols, axis=1, inplace=True)
    final = final.drop_duplicates()
    
    sum_values_and_aggregate = final.groupby(['numCDEClient'], as_index = False).agg({'nbcolis_unit': 'sum', 'vol_unit': 'sum', 'pds_unit': 'sum'}).set_index('numCDEClient')
    #Join dataframes
    final = final.set_index('numCDEClient').join(sum_values_and_aggregate, lsuffix='_l').reset_index(inplace=False).drop(['nbcolis_unit_l','vol_unit_l','pds_unit_l'], axis=1)
    #Drop duplicates and unnecessary columns
    final = final.drop_duplicates(subset=["numCDEClient"]).reset_index(inplace=False).drop(['index'], axis=1).reindex(new_cols, axis=1)

    #Save to excel file
    final.to_excel('import file-wuunder ' + today + '.xlsx', sheet_name='exported-data', index=None, engine='openpyxl')

    return final

def case_ambro():

    search = ['26360-01', '26342-01', '26268-01', '26252-01' ,'26220-01' ,'26160-01' ,'26063-01' ,'26059-01', '25922-01', '25868-01', '25540-01', '25768-01', '25768-01', '25503-01']
    new_cols = ['Odbiorca', 'Imię i Nazwisko', 'Telefon', 'Email', 'Adres*', 'Miasto*', 'Kod pocztowy', 'Kraj', 'Liczba paczek*', 'vol', 'Waga paczki', 'ref', 'cena', 'order']

    #Fix references
    temp = df3.rename(columns={'SKU':'ref'}, inplace=True)
    temp = df3.replace('BESO-', '', inplace=True, regex=True)

    trimmed_df1 = df1.loc[:, ['REF', 'LP', 'ORDER']].sort_values(by=['REF'])
    trimmed_df2 = df2.loc[:, ['ref', 'pack', 'vol', 'weight', 'cena']].sort_values(by=['ref'])

    temp = df3[df3['Order reference'].isin(search)].sort_values(by=['ref']) 

    temp = temp.join(trimmed_df1.set_index('REF'), on='ref', rsuffix='REF')
    temp = temp.join(trimmed_df2.set_index('ref'), on='ref', rsuffix='ref')

    #Reindex new dataframe
    final = pd.DataFrame([])
    final = temp.loc[:, :]
    final = final[['Order reference', 'Full name', 'Phone', 'Email', 'Address line 1', 'City', 'Postcode', 'Country', 'pack', 'vol', 'weight', 'ref', 'cena', 'ORDER']]
    final.set_axis(new_cols, axis=1, inplace=True)
    final = final.drop_duplicates()

    sum_values_and_aggregate = final.groupby(['Odbiorca'], as_index = False).agg({'Liczba paczek*': 'sum', 'vol': 'sum', 'Waga paczki': 'sum', 'cena': 'sum'}).set_index('Odbiorca')
    #Join dataframes
    final = final.set_index('Odbiorca').join(sum_values_and_aggregate, lsuffix='_l').reset_index(inplace=False).drop(['Liczba paczek*_l','vol_l','Waga paczki_l','cena_l'], axis=1)
    #Drop duplicates and unnecessary columns
    final = final.drop_duplicates(subset=['Odbiorca']).reset_index(inplace=False).drop(['index'], axis=1).reindex(new_cols, axis=1)

    #Concatenate values of columns
    final['Referencja'] = final['Odbiorca'].map(str) + ';' + final['Liczba paczek*'].values.astype(int).astype(str) + ';' + final['ref'].map(str) + ';' + final['order'].map(str)
    final = final.assign(**{'Rodzaj opakowania': 'MEBEL', 'Pobranie': '', 'Uwagi': 'WNIESIENIE+ROZPAKOWANIE+ZABRANIE OPAKOWAŃ'})

    #Add packages dimensions
    final['vol/pack'] = final['vol'].div(final['Liczba paczek*'])
    dimensions = final['Długość paczki'] = final['vol/pack'].pow((1/3)).multiply(100).round()
    final = final.assign(**{'Wysokość paczki': dimensions, 'Szerokość paczki': dimensions})
    final = final.drop(columns=['vol/pack', 'vol'])

    #Add insurance of the package
    final['Ubezpieczenie'] = final.apply(currency_convertor, axis=1).round()

    #Save to excel file
    final.to_excel('import file-ambro ' + today + '.xlsx', sheet_name='exported-data', index=None, engine='openpyxl')

    return final