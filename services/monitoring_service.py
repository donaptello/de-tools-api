import pandas as pd
import numpy as np

from datetime import datetime
from config.pg_config import JdbcConfig
from models.monitoring.monitoring_payload import MonitoringParameterPayload


class MonitoringService: 
    def __init__(self):
        self.__pg_obj: JdbcConfig = JdbcConfig()

    def get_param_mapping(self, name: str, flag: str, layer: str): 
        conn = self.__pg_obj.client_connect()
        filters = "1=1" if name is None else f"table_name_source LIKE '%%{name}%%'"
        flag_filters = "1=1" if flag is None else f"flag = '{flag}'"
        layer_filters = "1=1" if flag is None else f"layer = '{layer}'"

        df = pd.read_sql(
            f"""
                SELECT *
                FROM 
                etl_monitoring.count_mapping
                WHERE {filters} AND {layer_filters} AND {flag_filters}
            """,
            con=conn
        )
        df.rename(columns={'schema': 'schemas'}, inplace=True)
        df['insert_time'] = df['insert_time'].astype(str)
        df.replace({np.NaN: None}, inplace=True)
        conn.connection.close()
        return df.to_dict('records')
    
    def insert_param_mapping(self, payload: dict): 
        payload['schema'] = payload.pop('schemas')
        payload['layer'] = payload['layer'].value
        payload['flag'] = payload['flag'].value
        payload['insert_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn = self.__pg_obj.client_connect()
        df = pd.DataFrame([payload])
        df.to_sql(
            'count_mapping',
            con=conn,
            schema='etl_monitoring',
            if_exists='append',
            index=False
        )
        conn.connection.close()
        return payload
    
    def update_param_mapping(
        self, 
        table_name: str,
        layer: str,
        flag: str,
        payload: dict
    ):
        result_get = self.get_param_mapping(
            name=table_name,
            layer=layer,
            flag=flag
        )
        if not len(result_get): 
            return None
        
        data = result_get[0]
        payload['layer'] = payload['layer'].value
        payload['flag'] = payload['flag'].value

        conn = self.__pg_obj.client_connect_psycopg()
        cursor = conn.cursor()

        cursor.execute(
            f"""
                UPDATE 
                    etl_monitoring.count_mapping
                SET 
                    table_name_source = %s
                    schema = %s
                    db_source = %s
                    db_target = %s
                    column_date_name = %s
                    table_name_target = %s
                    data_source_column_name = %s
                    data_source = %s
                    layer = %s
                    flag = %s
                    insert_time = %s
                WHERE 
                    table_name_source LIKE '{table_name}'
                    AND flag = '{flag}'
                    AND layer = '{layer}'
            """,
            payload
        )
        row_updated = cursor.rowcount

        cursor.close()
        conn.close()

        return data
    
    def delete_param_mapping(self, table_name: str, flag: str, layer: str): 
        conn = self.__pg_obj.client_connect_psycopg()
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM connections WHERE table_name_source = '{table_name}' AND flag = '{flag}' AND layer = '{layer}';")
        result = cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()
        return result

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
