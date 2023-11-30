import pandas as pd
import pymssql 
import modin.pandas as pd1
import numpy as np
import datetime
import ast
import os
import json 
class chenshui():
    # 初始化共享参数
    def __init__(self) :
        self.server = '192.168.0.169'
        self.user = 'chjreport'
        self.password = 'Chj@12345'
        self.database = 'ChjBidb'
        # 动态时间
    def get_dynamic_dates(self):
        # 获取今天的日期
        today = datetime.date.today()
        # 获取本月的第一天
        first_day_of_month = today.replace(day=1)
        
        # 判断今天是否在本月的7天内
        if today.day <=3:
            # 如果是，将开始时间设置为上个月的1号
            start_date = (first_day_of_month - datetime.timedelta(days=1)).replace(day=1)
        else:
            # 如果不是，将开始时间设置为本月的1号
            start_date = first_day_of_month
        
        # 将结束时间设置为昨天
        end_date = today - datetime.timedelta(days=1)
        params = {"start_date":start_date,"end_date":end_date}
        # 返回开始时间和结束时间
        return params
    # url提取
    def generate_new_url(row):
        url = row['url']
        options_str = row['options']
        options_dict = json.loads(options_str)
        prodid = options_dict.get('prodid', '')
        RecoID = options_dict.get('RecoID', '')
        tabs = options_dict.get('tabs', '')
        query_parameters = []
        if tabs:
            query_parameters.append(f"tab={tabs}")
        if prodid:
            query_parameters.append(f"prodid={prodid}")
        if RecoID:
            query_parameters.append(f"RecoID={RecoID}")

        if query_parameters:
            new_url = f"{url}?{'&'.join(query_parameters)}"
            print(new_url)
            return new_url
        else:
            return url
    def dataget(self,params):
        with pymssql.connect(self.server, self.user, self.password, self.database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                # 兑礼数据
                sql1 = f'''
                    SELECT
                        mobile AS 手机号,
                        statuname AS 状态,
                        date_add AS 日期,
                        product_title AS 礼品 
                    FROM
                        v_mall_order_detail 
                    WHERE
                        title = '礼品' 
                        AND date_add BETWEEN '{params['start_date']}' 
                            AND '{params['end_date']}' + ' 23:59:59'
                        AND statuname != '关闭' 
                        AND statuname != '申请退货' 
                        AND statuname != '已退款' 
                    ORDER BY
                        date_add     
                '''
                cursor.execute(sql1)
                rows1 = cursor.fetchall()
                gift_data = pd.DataFrame(rows1, columns=['手机号', '状态', '日期', '礼品'])
                gift_data = gift_data[gift_data['状态'].isin(['关闭', '申请退货', '已退款']) == False]
                
                # 消费转化
                sql2 = f'''
                    SELECT DISTINCT
                        会员号 AS 手机号,
                        sum(金额) AS 转化金额
                    FROM
                        BI_Business_Consume 
                    WHERE
                        消费时间 BETWEEN '{params['start_date']}' 
                            AND '{params['end_date']}' + ' 23:59:59'
                    GROUP BY
                        会员号 
                '''
                cursor.execute(sql2)
                rows2 = cursor.fetchall()
                money_data = pd.DataFrame(rows2, columns=['手机号', '转化金额'])
                money_data['手机号'] = money_data['手机号'].str.strip().astype(str)
                
                # 激活
                sql3 = f'''
                    SELECT DISTINCT
                        电话 AS 手机号 
                    FROM
                        BI_Business_Member 
                    WHERE
                        首次绑定时间 BETWEEN '{params['start_date']}' 
                            AND '{params['end_date']}' + ' 23:59:59'
                '''
                cursor.execute(sql3)
                rows3 = cursor.fetchall()
                account_data = pd.DataFrame(rows3, columns=['手机号'])
                account_data = account_data.drop_duplicates()
                # 点击数据
                sql4 = f'''
                     SELECT
                            mobile as 手机号,url,options 
                        FROM
                            [dbo].[BI_Business_Visit_Record_Extension] 
                        WHERE
                            url = 'pages/detail/index' 
                            AND options LIKE '%RecoID%' 
                            AND options LIKE '%prodid%' 
                            AND date_add BETWEEN '{params['start_date']}' 
                            AND '{params['end_date']}' + ' 23:59:59'
                '''
                cursor.execute(sql4)
                rows4 = cursor.fetchall()
                click_data = pd.DataFrame(rows4)
                click_data['户外广告埋点'] = click_data.apply(self.generate_new_url, axis=1)
                click_data = click_data[click_data['户外广告埋点'].str.contains('prodid')]
                click_data = click_data[['手机号','户外广告埋点']]
    
        return gift_data, money_data, account_data,click_data

    # 数据处理函数
    def dataExtract(path):
        data = pd.read_excel(path)
        final_data = data[["场景","人群名称","手机号","触达","点击"]]
        return final_data

    # 获取本地数据
    def local_data(self,df,click_data):
        click_data_i = click_data[click_data["户外广告埋点"]==df['户外广告埋点'].unique()[0]]
        users_i = click_data_i["手机号"].tolist()
        df["点击"] = df["手机号"].apply(lambda x: 1 if x in users_i else 0)
        return df

    # 合并数据集
    def data_merge(self,platform_data,money_data,groups_data):
        mid_data = platform_data.merge(money_data,how='left',on="手机号")
        return mid_data

    # 获取用户名单
    def users_list(self,gift_data,money_data,account_data,platform_data):
        gift_id = gift_data["手机号"].unique().tolist()
        account_id = account_data["手机号"].unique().tolist()
        money_id = money_data["手机号"].unique().tolist()
        message_id = platform_data["手机号"].unique().tolist()
        users_list = {
            'gift':gift_id,
            'account':account_id,
            'money':money_id,
        }
        return users_list

    # 用户行为判断
    def data_fliter(data,users):
        if data in users:
            return 1
        else:
            return 0
    
    # 转化百分率
    def convert_to_percent(column):
        if column.name and '率' in column.name:
            return column.apply(lambda x: '{:.2%}'.format(x))
        else:
            return column

    # 数据处理
    def data_processing(self,mid_data,users_list):
        mid_data["兑礼"] = np.where(mid_data["手机号"].isin(users_list['gift']),1,0)
        mid_data["转化"] = np.where(mid_data["手机号"].isin(users_list['money']),1,0)
        mid_data["激活"] = np.where(mid_data["手机号"].isin(users_list['account']),1,0)
        mid_data["转化金额"].fillna(0,inplace=True)
        final_data =mid_data[['场景','人群名称','手机号','触达','点击', '兑礼','转化','转化金额','激活']]
        final_data.to_excel("final03.xlsx")
        # 统计不同人群名称的相关数据，并将结果存储到 final 中
        grouped1 = final_data.groupby(['场景','人群名称'])
        final1 = pd.DataFrame({
            '计划触达数': grouped1['手机号'].nunique(),
            '实际触达数': grouped1['触达'].sum(),
            '点击人数': grouped1['点击'].sum(),
            '点击率': (grouped1['点击'].sum()/grouped1['触达'].sum()).apply(lambda x: '{:.2%}'.format(x)),
            '转化人数': grouped1['转化'].sum(),
        }).reset_index()
        # final1['点击率'] = final1['点击人数']/final1['实际触达数']

        grouped2 = final_data[final_data["点击"]==1].groupby(['场景','人群名称'])
        final2 = pd.DataFrame({
            '点击后兑礼人数': grouped2['兑礼'].sum(),
            '点击后转化人数': grouped2['转化'].sum(),
            '点击后转化金额': grouped2["转化金额"].sum(),
            '点击后激活': grouped2["激活"].sum()
        }).reset_index()
        final2['点击后兑礼率'] = final2["点击后兑礼人数"]/final1["点击人数"]
        final2['点击后转化率'] = final2["点击后转化人数"]/final1["点击人数"]

        grouped3 = final_data[final_data["触达"]==1].groupby(['场景','人群名称'])
        final3 = pd.DataFrame({
            '整体兑礼人数': grouped3['兑礼'].sum(),
            '整体转化人数': grouped3['转化'].sum(),
            '整体转化金额': grouped3["转化金额"].sum(),
            '整体激活人数': grouped3['激活'].sum(),
        }).reset_index()
        final3['整体兑礼率'] = final3["整体兑礼人数"]/final1["实际触达数"]
        final3['整体转化率'] = final3["整体转化人数"]/final1["实际触达数"]


        merged = pd.merge(final1, final2, on=['人群名称','场景'], how='inner')
        final = pd.merge(merged, final3, on=['人群名称','场景'], how='inner')
        return final
    def save_results_to_excel(self,result_dict, file_name):
        with pd.ExcelWriter(file_name) as writer:
            for key, value in result_dict.items():
                sheet_name = '_'.join(key)
                value.to_excel(writer, sheet_name=sheet_name, index=False)
    # a function help me find all the files in the folder
    def find_all_files(self, path):
        files = []
        for root, dirs, file in os.walk(path):
            for f in file:
                files.append(os.path.join(root, f))
        return files
    # 汇总运行函数
    def run(tools):
        # 获取时间参数
        params = tools.get_dynamic_dates()
        # 获取数据库数据
        gift_data,money_data,account_data,click_data =tools.dataget(params)
        # 定义 Excel 文件名和子表名列表
        filename = tools.find_all_files('D:\\code\\沉睡触达\\org')
        # 读取 Excel 文件中的所有 Sheet
        xls_file = pd.read_excel(filename, sheet_name=None,usecols = ["场景", "人群名称", "手机号", "户外广告埋点", "触达"])
        dfs=[]
        for sheet_name, df in xls_file.items():
            dfs.append(df)
        data_list={}
        for sheet_name, df in xls_file.items():
            # 读取对应的 dataframe
            # 获取本地人群包数据，短信平台数据
            platform_data = tools.local_data(df,click_data)
            platform_data.to_excel(f'{sheet_name}_detail.xlsx')
            platform_data['手机号']= platform_data['手机号'].astype(str).str.strip()
            money_data['手机号']= money_data['手机号'].astype(str).str.strip()
            # 合并数据集
            mid_data = platform_data.merge(money_data,how='left',on="手机号")
            # 整理人群列表
            users_list = tools.users_list(gift_data,money_data,account_data,platform_data)
            # 处理最终数据
            final = tools.data_processing(mid_data,users_list)
            data_list[sheet_name] = final
            final.to_excel(f'{sheet_name}.xlsx')
if __name__== "__main__":
    # 初始化对象
    tools = chenshui()
    chenshui.run(tools)
