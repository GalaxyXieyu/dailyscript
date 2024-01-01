# chat_analysis.py

import os
import ast
import pandas as pd
from embedchain import App
from embedchain.config import ChunkerConfig, AddConfig, QueryConfig
from string import Template

def get_chat_records(start_date, end_date, mobile, db):
    sql = f'''
    select 发送人,接收人,发送时间,消息内容
    from 
    ((SELECT
            distinct s.lastname 发送人,
            d.MemberName 接收人,
            msgTime as 发送时间,
            body as 消息内容
    FROM
            Business_Chat_Message as m  
    JOIN
            base_staff as s on m.from2 = s.userid
    join Business_qywechatmember as q on q.externalUserId = m.toList
    join 
            business_member as d on d.wxUnionId = q.unionId
    WHERE
            d.MobileNumber = {mobile}
            AND ACTION = 'send' 
            AND msgTime between '{start_date}' and '{end_date} 23:59:59.59')
    union all
    (SELECT
            distinct d.MemberName 发送人,
            s.lastname 接收人,
            msgTime 发送时间,
            body 消息内容
    FROM
            Business_Chat_Message as m  
    JOIN
            base_staff as s on m.toList = s.userid
    join Business_qywechatmember as q on q.externalUserId = m.from2
    join 
            business_member as d on d.wxUnionId = q.unionId
    WHERE
            d.MobileNumber = {mobile}
            AND ACTION = 'send' 
            AND msgTime between '{start_date}' and '{end_date} 23:59:59.59' )) as t
    order by 发送时间 ASC 
    '''
    data = db.execute_sql(sql, column_names=['发送人', '接收人', '发送时间', '消息内容'], save_to_excel=False)
    data.drop_duplicates(inplace=True)
    data['消息内容'] = data['消息内容'].apply(lambda x: ast.literal_eval(x)['content'] if 'content' in ast.literal_eval(x) else None)
    conversations = []
    for index, row in data.iterrows():
        message = f"{row['发送人']}发送给{row['接收人']}：{row['消息内容']}"
        conversations.append(message)
    
    return "\n".join(conversations)

def analyze_comment(openai_key, chat_string):
    # 设置环境变量
    os.environ["OPENAI_API_KEY"] = openai_key

    # 初始化App
    elon_musk_bot = App()

    # 配置
    chunker_config = ChunkerConfig(chunk_size=2000, chunk_overlap=300, length_function=len)
    add_config = AddConfig(chunker=chunker_config)

    # 添加本地文本
    elon_musk_bot.add_local("text", chat_string, config=add_config)

    # 创建模板
    einstein_chat_template = Template("""
            你是一个专业的用户调研专家，是最能够从用户的表达中洞察他们的需求的人。
            使用一下我们的KOC项目这个小号和用户聊天，了解他们的需求。

            Context: $context
            记住，如果你不知道答案，请直接说你不知道，不要编造答案。
            Human: $query""")
    query_config = QueryConfig(template=einstein_chat_template, temperature=0.9, max_tokens=1000, top_p=1)

    query = '''
            这个是一个我们公司针对花丝工艺珠宝产品的用户调研，想要通过用户购买过程中的核心观点和业务建议来改进我们的产品和服务，分点越详细越好，
            请你按照以下的格式回答问题：
            【用户名】：
            【职业】：
            【产品优点】：
            【产品缺点】：
            【核心观点】：
            【业务优化建议】：
    '''
    
    response = elon_musk_bot.query(query, query_config)
    elon_musk_bot.reset()
    return response

if __name__ == "__main__":
    # 示例代码，你可以根据需要进行修改
    db = ...  # 你的数据库连接或实例
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    mobile = 13192288950
    chat_records = get_chat_records(start_date, end_date, mobile, db)
    print(chat_records)
