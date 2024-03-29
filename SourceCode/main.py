import time, os, traceback, requests
import pandas as pd
from pprint import pprint
from datetime import datetime
from func_tools import Category_TFN, word2num
pd.options.mode.chained_assignment = None  # default='warn'

t1 = time.time() # 起始時間

# 建立儲存 Open Data 的資料夾與處理篩選完輸出的資料夾
data_dir = './DATA/'
output_dir = './OUTPUT/'
for directory in [data_dir, output_dir]:
    if not os.path.exists(directory):
        os.mkdir(directory)

##### 第一題 #####
def Question_1():
    ### 城市方面
    # A 代表台北市
    # B 代表台中 
    # E 代表高雄
    # F 代表新北
    # H 代表桃園
    ### 代號方面
    # A 代表不動產
    # B 代表預售屋
    download_fileName_dict = {
        "A":["A", "E", "F"],
        "B":["B", "H"]
    }
    # Open Data 網址
    base_url = 'https://plvr.land.moi.gov.tw//DownloadSeason'
    # 由於是下載 106~109 年，故要 109+1
    for year in range(106, 109+1):
        # 下載四個季
        for season in range(1, 4+1):
            params = {
                'season':f"{year}S{season}"
            }
            # 依照城市代碼、資料類別代碼下載檔案
            for category_code, city_code_list in download_fileName_dict.items():
                for city_code in city_code_list:
                    params['fileName'] = f"{city_code}_lvr_land_{category_code}.csv"
                    pprint(params)
                    filename = f"{params['season']}__{params['fileName']}"
                    filepath = f"{data_dir}{filename}"
                    # 如果檔案已下載，略過下載
                    if os.path.exists(filepath):
                        continue
                    # 下載檔案並儲存檔案
                    rsp = requests.get(base_url, params=params)
                    rsp.close()
                    f = open(filepath, 'wb')
                    f.write(rsp.content)
                    f.close()

###### 第二題與第三題 ######
def Question_2__Question_3():
    tfn_class = Category_TFN()
    df_list = []
    file_list = os.listdir(data_dir)
    file_list.sort()
    for filename in file_list:
        # 處理檔名中有 _lvr_land_ 並且副檔名是 csv 的檔案
        if '_lvr_land_' not in filename \
        or not filename.lower().endswith('.csv'):
            continue
        # 讀取 CSV 檔案
        print(f"{data_dir}{filename}")
        # 109S3__B_lvr_land_B.csv 有一列有誤，要特別處理該列
        # 錯誤訊息是 「b'Skipping line 112: expected 28 fields, saw 29\n'」
        if 'B_lvr_land_B' in filename \
        and '109S3' in filename:
            f = open(f"{data_dir}{filename}", 'r')
            content = f.read()
            f.close()
            if '太平區,房地(土地+建物)+車位,新光段961~990地號,38.89,住,,,1041214,土地1建物1車位4,一層,13,店面(店鋪),住家用,鋼筋混凝土造,,278.14,0,0,0,無,無,20950000,115216,坡道平面,141.44,5200000,建物型態為店面,購買一、二層,RPORMLTLNHPFFJB07CB' in content:
                content = content.replace(
                    '太平區,房地(土地+建物)+車位,新光段961~990地號,38.89,住,,,1041214,土地1建物1車位4,一層,13,店面(店鋪),住家用,鋼筋混凝土造,,278.14,0,0,0,無,無,20950000,115216,坡道平面,141.44,5200000,建物型態為店面,購買一、二層,RPORMLTLNHPFFJB07CB',
                    '太平區,房地(土地+建物)+車位,新光段961~990地號,38.89,住,,,1041214,土地1建物1車位4,一層,13,店面(店鋪),住家用,鋼筋混凝土造,,278.14,0,0,0,無,無,20950000,115216,坡道平面,141.44,5200000,建物型態為店面購買一、二層,RPORMLTLNHPFFJB07CB'
                )
                f = open(f"{data_dir}{filename}", 'w')
                f.write(content)
                f.close()
        # 如果檔案無法以 pandas 讀取，代表檔案有誤，error_bad_lines 可略過格式有錯誤的 row
        try:
            df = pd.read_csv(f"{data_dir}{filename}", error_bad_lines=False)
        except:
            print(f"{data_dir}{filename} is unreadable.")
            print(traceback.format_exc())
            continue
        # 如果資料表示空的，略過該檔案
        if df.empty:
            print(f"{data_dir}{filename} is empty.")
            continue
        # 將第二列的欄位英文名稱取出
        rename_cols_dict = df.iloc[0].to_dict()
        # 106S2__B_lvr_land_B.csv 無法取得正確內容，讀取上會有問題，故要略過該檔案
        if len(list(rename_cols_dict.keys()))<=1:
            print(f"{data_dir}{filename} 檔案有問題，略過")
            continue
        # 修改欄位名稱，從中文改成英文
        df = df.rename(columns=rename_cols_dict)
        # 去除資料第一列「欄位的英文名稱」
        df = df.drop(df.index[0])
        # 新增一名為 df_name 的欄位
        df['df_name'] = f"{'_'.join(filename.split('__')[0].split('S'))}_{filename.split('__')[1].split('.')[0].split('_')[0]}_{filename.split('__')[1].split('.')[0].split('_')[-1]}"

        # 正規化總樓層數的欄位內容，並加入 df_list 中
        df_list.append(tfn_class.execute(filename.split('__')[1].split('.')[0].split('_')[-1], df))

    ###### 第三題 ######
    # 將所有資料組合在一起
    df_all = pd.concat(df_list)
    # 這邊要存成 pkl 不能存成 csv，不然爾後讀取時，會把樓層資訊存成浮點數
    df_all.to_pickle(f"{output_dir}df_all.pkl")

###### 第四題 ######
def Question_4():
    if not os.path.exists(f"{output_dir}df_all.pkl"):
        return
    else:
        df_all = pd.read_pickle(f"{output_dir}df_all.pkl")
        os.remove(f"{output_dir}df_all.pkl")
    # 將需要篩選的資料篩選後輸出成 filter.csv
    # 篩選條件有 main use 是 住家用、building state 是 住宅大樓、total floor number 非 Nan 且大於等於 13
    df_q1 = df_all[(df_all['main use']=='住家用') & (df_all['building state']=='住宅大樓(11層含以上有電梯)') & (df_all['total floor number']!=float('nan')) & (df_all['total floor number']>=13)]
    # 輸出篩選結果為 filter.csv
    df_q1.to_csv(f"{output_dir}filter.csv", index=False)

    # 將需要統計的資料輸出成 count.csv
    data_dict = {
        '總件數'       : [df_all.shape[0]],
        '總車位數'      : [sum([int(cell_content.split('車位')[1]) for cell_content in df_all['transaction pen number']])],
        '平均總價元'    : [int(sum([int(cell_content) for cell_content in df_all['total price NTD']])/df_all.shape[0])],
    }

    # 有些不動產買賣，有賣車位，但「車位總價元」為 0，故統計平均車位總價元時，要去除這狀況
    total_berth_price = 0
    total_berth_count = 0
    for index, cell_content in enumerate(df_all['the berth total price NTD']):
        berth_price = int(cell_content)
        berth_count = int(df_all.iloc[index]['transaction pen number'].split('車位')[1])
        if not berth_price:
            continue
        elif not berth_count:
            continue
        total_berth_price+=berth_price
        total_berth_count+=berth_count
    data_dict['平均車位總價元'] = [int(total_berth_price/total_berth_count)]

    # 建立 count.csv 的 DataFrame
    df_q2 = pd.DataFrame(data=data_dict)
    # 輸出 count.csv
    df_q2.to_csv(f"{output_dir}count.csv", index=False)

###### 第五題 ######
def Question_5():
    if not os.path.exists(f"{output_dir}filter.csv") \
    or not os.path.exists(f"{output_dir}count.csv"):
        return
    df_q1 = pd.read_csv(f"{output_dir}filter.csv", error_bad_lines=False)
    df_q2 = pd.read_csv(f"{output_dir}count.csv", error_bad_lines=False)
    
    for column_name, cell_content in df_q2.iloc[0].to_dict().items():
        df_q1[column_name] = cell_content

    # 將第四題的篩選結果輸出為 result.csv
    df_q1.to_csv(f"{output_dir}result.csv", index=False)

Question_1()
Question_2__Question_3()
Question_4()
Question_5()

# print 出全部執行完成後所耗費的秒數
print(f"執行時間耗費 {round(time.time()-t1, 3)} 秒")