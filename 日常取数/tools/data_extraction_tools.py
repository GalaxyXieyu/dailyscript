import pymssql
import pandas as pd
import datetime
import hashlib
import os
import requests
from langchain.output_parsers import StructuredOutputParser, ResponseSchema
from langchain.prompts import PromptTemplate
from langchain.llms import OpenAI

class Database:
    """
    数据库类，用于执行SQL查询并解析结果。
    """
    def __init__(self, server, user, password, database, openai_api_key):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.openai_api_key = openai_api_key

    def decode_columns(self, df, column_names):
        """
        对指定列进行解码。
        """
        if column_names:
            for column_name in column_names:
                mask = df[column_name].notnull()
                try:
                    df.loc[mask, column_name] = df.loc[mask, column_name].apply(lambda x: x.encode('latin1').decode('gbk'))
                except:
                    pass
        return df

    def execute_sql(self, sql, column_names=None, save_to_excel=False):
        """
        执行SQL查询并返回结果。
        """
        if '*' in sql:
            raise ValueError("Please specify column names in your SQL query instead of using '*'.")

        with pymssql.connect(self.server, self.user, self.password, self.database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute(sql)
                data = pd.DataFrame(cursor.fetchall())
                data = self.decode_columns(data, column_names)
                if save_to_excel:
                    now = datetime.datetime.now()
                    hash_object = hashlib.sha256(sql.encode())
                    hex_dig = hash_object.hexdigest()
                    file_id = hex_dig[:4]
                    try:
                        os.mkdir("data")
                    except:
                        pass
                    filename = f"{now.strftime('%Y%m%d')}_id_{file_id}"
                    data.to_excel(f"./data/{filename}.xlsx", index=False)
                    with open(f"./data/{filename}.txt", "w") as f:
                        f.write(sql)
                return data

    async def parse_sql(self, sql):
        """
        使用OpenAI解析SQL查询。
        """
        response_schemas = [
            ResponseSchema(name="bg", description="解析这个SQL获取的信息是什么？结合他筛选的条件和内容，回答这个问题。"),
            ResponseSchema(name="target_columns", description="这个sql最终要返回的列名是什么？"),
            ResponseSchema(name="fliter_conditions", description="这个sql的筛选条件是什么？"),
            ResponseSchema(name="relative_tables", description="这个sql查询涉及到的表是什么？")
        ]
        output_parser = StructuredOutputParser.from_response_schemas(response_schemas)
        format_instructions = output_parser.get_format_instructions()
        prompt = PromptTemplate(
            template="\n{sql} 记住你是一个sqlserver高手，尽你最大的能力用中文通俗地分析这个sql语句 \n{format_instructions}",
            input_variables=["sql"],
            partial_variables={"format_instructions": format_instructions}
        )
        model = OpenAI(temperature=0, openai_api_key=self.openai_api_key)
        _input = prompt.format_prompt(sql=sql)
        output = model(_input.to_string())
        return output_parser.parse(output)

    async def save_sql_and_result(self, sql):
        """
        保存SQL查询和其解析结果。
        """
        try:
            result = await self.parse_sql(sql)
        except:
            result = "解析失败"
        now = datetime.datetime.now()
        hash_object = hashlib.sha256(sql.encode())
        hex_dig = hash_object.hexdigest()
        file_id = hex_dig[:4]
        try:
            os.mkdir("data")
        except:
            pass
        filename = f"{now.strftime('%Y%m%d')}_id_{file_id}"
        with open(f"./data/{filename}.txt", "w") as f:
            f.write(f"SQL: {sql}\n\nResult: {result}")

def download_and_save_images(data, category_column, code_column, image_url_column, save_directory="./data/TOP10产品"):
    """
    下载并保存图片到本地。
    """
    if not os.path.exists(save_directory):
        os.makedirs(save_directory)
    
    for i, row in data.iterrows():
        url = row[image_url_column]
        response = requests.get(url)
        filename = f"{row[category_column]}{i + 1}_{row[code_column]}.jpg"
        file_path = os.path.join(save_directory, filename)
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f'<img src="{file_path}" alt="图片描述" style="width: 100px; height: 100px; float: left;">')
