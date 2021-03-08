import math, traceback
import pandas as pd
from copy import deepcopy
class Category_TFN():
    def __init__(self):
        self.tfn_dict = {
            'A': self.category_A,
            'B':self.category_B,
        }
        # 樓層中文轉數字等對照用 dict
        self.floor_chn2int = {
            float('nan'):float('nan'),
            '地下層':float('nan'),
        }
        # 設定一樓到一千樓的 str 與 int 對照
        for i in range(1, 1001):
            self.floor_chn2int[f"{i}"] = i

    def execute(self, category_code, dataframe):
        if category_code not in self.tfn_dict:
            raise Exception('Invalid category_code')
        return self.tfn_dict[category_code](dataframe)

    def category_A(self, dataframe):
        # 處理樓層數是中文的狀況，如「三十五層」、「十三層」、「地下層」
        for index, cell_content in enumerate(dataframe['total floor number']):
            index+=1
            # 處理樓層數是 Nan 的狀況
            if type(cell_content) in [int, float]\
            and math.isnan(cell_content):
                continue
            if cell_content not in self.floor_chn2int:
                status = False
                # 處理樓層數是 「十三層」、「三十七層」的狀況
                if '層' in dataframe['total floor number'][index] \
                and dataframe['total floor number'][index] not in self.floor_chn2int:
                    try:
                        self.floor_chn2int[cell_content] = deepcopy(word2num(dataframe['total floor number'][index].replace('層', '')))
                        status = True
                    except:
                        print(traceback.format_exc())
                if not status:
                    print(
                        '\n'.join(
                            [
                                '-'*20,
                                f"{dataframe['total floor number'][index]}",
                                f"{type(dataframe['total floor number'][index])}",
                                '-'*20,
                            ]
                        )
                    )
                    self.floor_chn2int[cell_content] = float('nan')
            dataframe['total floor number'][index] = self.floor_chn2int[cell_content]
        return dataframe

    def category_B(self, dataframe):
        dataframe['total floor number'] = pd.to_numeric(dataframe['total floor number'], downcast='integer')
        return dataframe

zh2digit_table = {'零': 0, '一': 1, '二': 2, '兩': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10, '百': 100, '千': 1000, '〇': 0, '○': 0, '○': 0, '０': 0, '１': 1, '２': 2, '３': 3, '４': 4, '５': 5, '６': 6, '７': 7, '８': 8, '９': 9, '壹': 1, '貳': 2, '參': 3, '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8, '玖': 9, '拾': 10, '佰': 100, '仟': 1000, '萬': 10000, '億': 100000000}
def word2num(zh_num):
    # 位數遞增，由高位開始取
    digit_num = 0
    # 結果
    result = 0
    # 暫時存儲的變量
    tmp = 0
    # 億的個數
    billion = 0
    while digit_num < len(zh_num):
        tmp_zh = zh_num[digit_num]
        tmp_num = zh2digit_table.get(tmp_zh, None)
        if tmp_num == 100000000:
            result = result + tmp
            result = result * tmp_num
            billion = billion * 100000000 + result
            result = 0
            tmp = 0
        elif tmp_num == 10000:
            result = result + tmp
            result = result * tmp_num
            tmp = 0
        elif tmp_num >= 10:
            if tmp == 0:
                tmp = 1
            result = result + tmp_num * tmp
            tmp = 0
        elif tmp_num is not None:
            tmp = tmp * 10 + tmp_num
        digit_num += 1
    result = result + tmp
    result = result + billion
    return result