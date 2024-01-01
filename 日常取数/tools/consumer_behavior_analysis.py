import pandas as pd
from datetime import datetime, timedelta
from datetime import datetime

def generate_time_parameters():
    # 获取当前日期
    today = datetime.now()
    # 当前年度的第一天
    start_time = datetime(today.year, 1, 1)

    # 当前日期的前一天
    end_time = today - timedelta(days=1)

    # 计算去年同期的开始和结束时间
    last_year_start = start_time.replace(year=start_time.year - 1)
    last_year_end = end_time.replace(year=end_time.year - 1)

    # 计算前年同期的开始和结束时间
    year_before_last_start = start_time.replace(year=start_time.year - 2)
    year_before_last_end = end_time.replace(year=end_time.year - 2)

    return start_time, end_time, last_year_start, last_year_end, year_before_last_start, year_before_last_end

def aggregate_and_export(data: pd.DataFrame, exclude_columns: list) -> pd.DataFrame:
    """
    对除指定列之外的其他列进行聚合处理。
    
    参数：
    - data: 输入的DataFrame。
    - exclude_columns: 需要排除的列名列表。
    
    返回值：
    - DataFrame，其中包含汇总的结果。
    """
    
    # 获取除了排除列之外的其他列名
    columns_to_process = [col for col in data.columns if col not in exclude_columns]
    
    # 使用groupby进行聚合操作
    aggregated_data = data.groupby(['会员号']).agg({col: lambda x: sorted(set(x)) for col in columns_to_process}).reset_index()
    
    # 将指定的列转换为str格式
    for col in columns_to_process:
        aggregated_data[col] = aggregated_data[col].astype(str)
    return aggregated_data, columns_to_process

def process_column_series(data: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    处理指定列的数据并返回汇总结果。

    参数：
    - data: 输入的DataFrame。
    - column_name: 需要进行处理的列名。

    返回值：
    - DataFrame，其中包含汇总的结果。
    """
    
    # 将字符串形式的列表转换为真正的列表
    data[column_name] = data[column_name].apply(eval)
    
    # 将列表中的字符串元素连接起来
    data[column_name] = data[column_name].apply(lambda x: '&'.join(x) if isinstance(x, list) else x)
    
    # 进行groupby操作
    agg = data.groupby([column_name])['会员号'].agg(会员数='count').reset_index()
    
    # 增加一列系列数，即指定列中包含的&号数量+1
    agg['系列数'] = agg[column_name].apply(lambda x: x.count('&') + 1)
    
    # 按照会员数降序排列
    agg.sort_values(by='会员数', ascending=False, inplace=True)
    
    # 排除系列数为1的指定列
    agg = agg[agg['系列数'] > 1]
    
    # 增加一列计算每种组合的占比
    agg['占比'] = agg['会员数'] / agg['会员数'].sum()
    return agg

def save_to_excel_sheets(data: pd.DataFrame, columns: list, filename: str) -> None:
    """
    循环获取每个会员的消费行为组合偏好并把agg，还有每个列的偏好占比用pandas写入同一个excel中的不同sheet。
    
    参数：
    - data: 输入的DataFrame。
    - columns: 需要进行处理的列名列表。
    - filename: 保存的Excel文件名。
    
    返回值：
    - 无。处理后的数据将被导出为指定的Excel文件。
    """
    
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    for col in columns:
        result = process_column_series(data, col)
        result.to_excel(writer, sheet_name=col, index=False)
    writer.close()
