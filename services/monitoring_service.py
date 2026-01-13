import pandas as pd
import numpy as np

from config.pg_config import JdbcConfig


class MonitoringService: 
    def __init__(self):
        self.__pg_obj: JdbcConfig = JdbcConfig()

    def get_param_mapping(self, name: str, flag: str): 
        conn = self.__pg_obj.client_connect()
        filters = "1=1" if name is None else f"table_name LIKE '%%{name}%%'"
        layer_filters = "1=1" if flag is None else f"flag = {flag}"

        df = pd.read_sql(
            f"""
                SELECT *
                FROM 
                etl_monitoring.count_mapping
                WHERE {filters} AND {layer_filters}
            """,
            con=conn
        )
        df.rename(columns={'schema': 'schemas'}, inplace=True)
        df['insert_time'] = df['insert_time'].astype(str)
        df.replace({np.NaN: None}, inplace=True)
        conn.connection.close()
        return df.to_dict('records')

    def get_monitoring(self, name: str): 
        conn = self.__pg_obj.client_connect()
        filters = "1=1" if name is None else f"table_name LIKE '%%{name}%%'"

        df = pd.read_sql(
            f"""
                SELECT *, 
                CASE 
                    WHEN total_in_source IS NULL OR total_in_target IS NULL 
                        THEN 'To Be Checked'
                    WHEN total_in_source > total_in_target
                        THEN 'InCompleted'
                    WHEN total_in_source < total_in_target
                        THEN 'To Be Checked'
                    ELSE 'Completed'
                end AS status
                FROM 
                etl_monitoring.daily_count_summary
                WHERE {filters}
            """,
            con=conn
        )
        df['lastrun'] = df['lastrun'].astype(str)
        df['date'] = df['date'].astype(str)
        df.replace({np.NaN: None}, inplace=True)
        conn.connection.close()
        return df.to_dict('records')
    
    def get_total_widget(self): 
        query = """
            with incompleted as  (
                select count(*) as total,
                'incompleted' as status
                from etl_monitoring.daily_count_summary
                where total_in_source > total_in_target
            ), to_be_checked as (
                select count(*) as total,
                'to_be_checked' as status
                from etl_monitoring.daily_count_summary
                where total_in_source < total_in_target OR total_in_source IS NULL OR total_in_target IS NULL 
            ), completed as (
                select count(*) as total,
                'completed' as status
                from etl_monitoring.daily_count_summary
                where total_in_source = total_in_target
            ), total_table as (
                select count(*) as total,
                'total_table' as status
                from etl_monitoring.daily_count_summary
            )select 
                total, 
                status
            from incompleted 
            union all 
            select 
                total, 
                status
            from completed 
            union all
            select 
                total, 
                status
            from to_be_checked
            union all 
            select 
            total, 
            status
            from total_table
        """
        conn = self.__pg_obj.client_connect()
        df = pd.read_sql(
            query, 
            con=conn
        )
        conn.connection.close()
        return df.to_dict('records')

    def get_monitoring_detail(self, name: str, limit: int): 
        conn = self.__pg_obj.client_connect()

        df = pd.read_sql(
            f"""
                SELECT * FROM 
                etl_monitoring.daily_count_detail
                WHERE table_name = '{name}'
                ORDER BY date DESC, lastrun DESC
                LIMIT {limit}
            """,
            con=conn
        )
        df.replace({np.NaN: None}, inplace=True)
        df['lastrun'] = df['lastrun'].astype(str)
        df['date'] = df['date'].astype(str)
        conn.connection.close()
        return df.to_dict('records'), df.shape[0]
