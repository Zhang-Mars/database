def unify_date(date):
    import datetime
    from datetime import datetime
    import pandas as pd
    import re
    pattern= re.compile('\d{4}/\d{1,2}/\d{1,2}')
    pattern2= re.compile('\d{1,2}/\d{1,2}')
    pattern3= re.compile(r'\d{1,2}[\u4e00-\u9fff]\d{1,2}[\u4e00-\u9fff]')
    pattern4= re.compile('\d{1,2}-\d{1,2}')
    pattern5=re.compile(r'\d{4}\s[\u4e00-\u9fff]\s\d{1,2}\s[\u4e00-\u9fff]\s\d{1,2}\s[\u4e00-\u9fff]')
    if pattern.search(date):
        date=pattern.search(date).group()
        today = pd.to_datetime('today')
        current_year = today.year

        # 解析日期並組合成完整的日期（默認設置為今年）
        # full_date_str = f"{current_year}/{date}"

        # 轉換為 datetime 格式
        date = pd.to_datetime(date, format='%Y/%m/%d').replace(hour=23, minute=59, second=59)

        # 如果該日期在今天之前，則設定為明年
        if date < today:
            date = date.replace(year=current_year + 1)
        year = pd.to_datetime('today').year
        date=pd.to_datetime(date)

        # date=date.strftime('%Y-%m-%d')
        # date=datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
    elif pattern2.search(date):
        date=pattern2.search(date).group()
        today = pd.to_datetime('today')
        current_year = today.year

        # 解析日期並組合成完整的日期（默認設置為今年）
        date = f"{current_year}/{date}"

        # 轉換為 datetime 格式
        date = pd.to_datetime(date, format='%Y/%m/%d').replace(hour=23, minute=59, second=59)

        # 如果該日期在今天之前，則設定為明年
        if date < today:
            date = date.replace(year=current_year + 1)
        year = pd.to_datetime('today').year
        date=pd.to_datetime(date)

        # date=date.strftime('%Y-%m-%d')
        # date=datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
    elif pattern4.search(date):
        date=pattern4.search(date).group()
        date = date.replace('-','/')
        today = pd.to_datetime('today')
        current_year = today.year

        # 解析日期並組合成完整的日期（默認設置為今年）
        date = f"{current_year}/{date}"

         # 轉換為 datetime 格式
        date = pd.to_datetime(date, format='%Y/%m/%d').replace(hour=23, minute=59, second=59)

        # 如果該日期在今天之前，則設定為明年
        if date < today:
            date = date.replace(year=current_year + 1)
        year = pd.to_datetime('today').year
        date=pd.to_datetime(date)
    elif pattern3.search(date):
        date=pattern3.search(date).group()
        date = re.sub(r'[\u4e00-\u9fff]', '/', date)[:-1]
        today = pd.to_datetime('today')
        current_year = today.year

        # 解析日期並組合成完整的日期（默認設置為今年）
        date = f"{current_year}/{date}"

        # 轉換為 datetime 格式
        date = pd.to_datetime(date, format='%Y/%m/%d').replace(hour=23, minute=59, second=59)

        # 如果該日期在今天之前，則設定為明年
        if date < today:
            date = date.replace(year=current_year + 1)
        year = pd.to_datetime('today').year
        date=pd.to_datetime(date)
    elif pattern5.search(date):
        date=date.replace(' ','')
        date = re.sub(r'[\u4e00-\u9fff]', '/', date)[:-1]
        date = pd.to_datetime(date, format='%Y/%m/%d').replace(hour=23, minute=59, second=59)
    return date

def remove_space(x):
    full_width_punctuations = {
    '。': '.',  # 全形句號 -> 半形句號
    '，': ',',  # 全形逗號 -> 半形逗號
    '！': '!',  # 全形驚嘆號 -> 半形驚嘆號
    '？': '?',  # 全形問號 -> 半形問號
    '：': ':',  # 全形冒號 -> 半形冒號
    '；': ';',  # 全形分號 -> 半形分號
    '（': '(',  # 全形左括號 -> 半形左括號
    '）': ')',  # 全形右括號 -> 半形右括號
    '【': '[',  # 全形左中括號 -> 半形左中括號
    '】': ']',  # 全形右中括號 -> 半形右中括號
    '《': '<',  # 全形左尖括號 -> 半形左尖括號
    '》': '>',  # 全形右尖括號 -> 半形右尖括號
    '「': '"',  # 全形左引號 -> 半形引號
    '」': '"',  # 全形右引號 -> 半形引號
    '、': ',',  # 全形頓號 -> 半形逗號
}
    x=x.replace(' ','')
    translation_table = str.maketrans(full_width_punctuations)
    x=x.translate(translation_table)
    return x
def call_dataframe():
    import mysql.connector
    import pandas as pd
    db_config = {
        'host': 'u3r5w4ayhxzdrw87.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
        'user': 'dhv81sqnky35oozt',
        'password': 'rrdv8ehsrp8pdzqn',
        'database': 'xltc236odfo1enc9',
        'charset': 'utf8mb4'
    }
    connection = mysql.connector.connect(**db_config)
    if connection.is_connected():
        print("成功連接到 MariaDB 資料庫")
    cursor=connection.cursor()
    cursor.execute('SELECT * FROM movies_html ORDER BY id DESC LIMIT 1')
    result=cursor.fetchall()
    res=result[0][1]
    final_data=pd.read_html(res)[0]
    cursor.execute('SELECT * FROM vieshow_html ORDER BY id DESC LIMIT 1')
    result=cursor.fetchall()
    res=result[0][1]
    vieshow_data=pd.read_html(res)[0]
    # vieshow_data['日期']=vieshow_data['日期'].apply(unify_date)
    final_data=pd.concat([vieshow_data,final_data])
    final_data['日期']=final_data['日期'].apply(unify_date)
    final_data['日期'] = pd.to_datetime(final_data['日期'])
    cinema_to_be_fill=final_data.groupby('電影院名稱').count().index
    columns_to_be_filled=['導演','演員','類型','宣傳照']
    # final_data['中文片名']=final_data['中文片名'].apply(remove_space)
    for cinema in cinema_to_be_fill:
        to_fill=final_data[final_data['電影院名稱']==cinema]
        ch_names=to_fill.groupby('中文片名').count().index
        for ch_name in ch_names:
            for col in columns_to_be_filled:
                final_data[col][(final_data[col].isna()) & (final_data['中文片名'].str.contains(ch_name,case=False))]=final_data[col][(final_data[col].isna()) & (final_data['中文片名'].str.contains(ch_name,case=False))].fillna(value=to_fill[[col,'中文片名']][to_fill['中文片名']==ch_name].iloc[0][col])
    return final_data
# che=call_dataframe()
