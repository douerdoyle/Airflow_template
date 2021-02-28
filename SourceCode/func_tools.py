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