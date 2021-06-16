import os
import pandas as pd
import requests

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  # This is your Project Root
my_dir = os.path.join(ROOT_DIR, r'data/')  # requires `import os`


def update_DBs():
    ieee_URLs = ['http://standards-oui.ieee.org/oui/oui.txt',
                 'http://standards-oui.ieee.org/oui28/mam.txt',
                 'http://standards-oui.ieee.org/oui36/oui36.txt',
                 'http://standards-oui.ieee.org/cid/cid.txt',
                 'http://standards-oui.ieee.org/iab/iab.txt']
    for url in ieee_URLs:
        filename = url[30:].split('/')[0]
        myfile = requests.get(url)
        open(my_dir + filename + '.txt', 'wb').write(myfile.content)
    print("update done!")


def create_DBs_csv():
    for filename in os.listdir(my_dir):
        DB_list = []
        with open(my_dir + filename, encoding="utf8") as infile:
            for line in infile:
                if "(hex)" in line:
                    try:
                        mac, vendor = line.strip().split("(hex)")
                        DB_list.append([mac.strip().replace("-", ":").lower(), vendor.strip()])
                        # print(OUI_list)
                    except:
                        mac = vendor = ''
                        print('ERROR IN LINE: ', line)
        df = pd.DataFrame(DB_list)
        df.columns = ['OUI', 'vendor']
        df.to_csv(my_dir + filename.rstrip('.txt') + '.csv', header=True, index=False)
    return


def mac_lookup(mac):
    oui = mac[:8]
    priority_DB = ['oui.csv', 'cid.csv']  # 'oui28.csv', 'oui36.csv', 'iab.csv']

    for filename in priority_DB:
        df = pd.read_csv(my_dir + filename)
        df = df.set_index('OUI')

        try:
            return df.at[oui, 'vendor']
        except:
            pass

    return 'not_resolved'


def df_mac_lookup(df):
    df_oui = pd.read_csv(my_dir + 'oui.csv')
    df_cid = pd.read_csv(my_dir + 'cid.csv')
    df_oui = df_oui.append(df_cid, ignore_index=True).sort_values(by='OUI')
    if not 'OUI' in df.columns:
        df['OUI'] = df.mac.apply(lambda x: x[:8])
    df = pd.merge(df, df_oui, how='left', on=['OUI'])
    df['vendor'] = df.vendor.fillna('not_resolved')
    return df



def lookup_raw_wifi(df):
    df_oui = pd.read_csv(my_dir + 'oui.csv')
    df_cid = pd.read_csv(my_dir + 'cid.csv')
    df_oui = df_oui.append(df_cid, ignore_index=True).sort_values(by='OUI')
    del df_cid
    df = pd.merge(df, df_oui, how='left', on=['OUI'])
    df = df.drop('OUI', axis=1).fillna('not_resolved')
    del df_oui
    return df


if __name__ == '__main__':
    update_DBs()
    df = pd.DataFrame({
        'mac': ['08:61:95:a9:74:8a','5c:86:13:67:54:34','90:18:7c:56:23:78','da:a1:19:89:23:67','d9:a1:19:89:23:67'],
        'time': pd.Timestamp('20130102')
        })

    print(df_mac_lookup(df))
    print(mac_lookup('08:61:95:a9:74:8a'))
