import pandas as pd
import numpy as np

from datetime import datetime
from loguru import logger
from config.pg_config import JdbcConfig
from models.monitoring.monitoring_payload import MonitoringParameterPayload


class MonitoringService: 
    def __init__(self):
        self.__pg_obj: JdbcConfig = JdbcConfig()

    def get_param_mapping(self, name: str, flag: str, layer: str): 
        conn = self.__pg_obj.client_connect()
        filters = "1=1" if name is None else f"table_name_source LIKE '%%{name}%%'"
        flag_filters = "1=1" if flag is None else f"flag = '{flag}'"
        layer_filters = "1=1" if layer is None else f"layer = '{layer}'"
        print(layer_filters, layer)

        df = pd.read_sql(
            f"""
                SELECT *
                FROM 
                etl_monitoring.count_mapping
                WHERE {filters} AND {layer_filters} AND {flag_filters}
                ORDER BY insert_time DESC
            """,
            con=conn
        )
        df.rename(columns={'schema': 'schemas'}, inplace=True)
        df['insert_time'] = df['insert_time'].astype(str)
        df.replace({np.NaN: None}, inplace=True)
        conn.connection.close()
        return df.to_dict('records')
    
    def __mapping_results(self, results: list, flag: str):
        results_mapped = {}
        for result in results: 
            if flag == 'source': 

                key = f"{result['table_name_source']}_{result['schemas']}_{result['db_source']}"
                if key in results_mapped:
                    if result['id_2'] is None:
                        continue

                    results_mapped[key]['target'].append(
                        {
                            "id": result['id_2'],
                            "table_name_source": result['table_name_source_2'],
                            "schemas": result['schema_2'],
                            "db_source": result['db_source_2'],
                            "db_target": result['db_target_2'],
                            "column_date_name": result['column_date_name_2'],
                            "table_name_target": result['table_name_target_2'],
                            "data_source_column_name": result['data_source_column_name_2'],
                            "data_source": result['data_source_2'],
                            "layer": result['layer_2'],
                            "flag": result['flag_2'],
                            "insert_time": result['insert_time_2'],
                        } 
                    )
                    continue

                results_mapped[key] = {
                    "source": {
                            "id": result['id'],
                            "table_name_source": result['table_name_source'],
                            "schemas": result['schemas'],
                            "db_source": result['db_source'],
                            "db_target": result['db_target'],
                            "column_date_name": result['column_date_name'],
                            "table_name_target": result['table_name_target'],
                            "data_source_column_name": result['data_source_column_name'],
                            "data_source": result['data_source'],
                            "layer": result['layer'],
                            "flag": result['flag'],
                            "insert_time": result['insert_time'],
                        },
                    "target": []
                }
                if result['id_2'] is not None: 
                    results_mapped[key]['target'].append({
                        "id": result['id_2'],
                        "table_name_source": result['table_name_source_2'],
                        "schemas": result['schema_2'],
                        "db_source": result['db_source_2'],
                        "db_target": result['db_target_2'],
                        "column_date_name": result['column_date_name_2'],
                        "table_name_target": result['table_name_target_2'],
                        "data_source_column_name": result['data_source_column_name_2'],
                        "data_source": result['data_source_2'],
                        "layer": result['layer_2'],
                        "flag": result['flag_2'],
                        "insert_time": result['insert_time_2'],
                    })
            else: 

                key = f"{result['table_name_target']}_{result['db_target']}"
                if key in results_mapped:
                    if result['id_2'] is None:
                        continue
                    results_mapped[key]['source'].append(
                        {
                            "id": result['id_2'],
                            "table_name_source": result['table_name_source_2'],
                            "schemas": result['schema_2'],
                            "db_source": result['db_source_2'],
                            "db_target": result['db_target_2'],
                            "column_date_name": result['column_date_name_2'],
                            "table_name_target": result['table_name_target_2'],
                            "data_source_column_name": result['data_source_column_name_2'],
                            "data_source": result['data_source_2'],
                            "layer": result['layer_2'],
                            "flag": result['flag_2'],
                            "insert_time": result['insert_time_2'],
                        }
                    )
                    continue

                results_mapped[key] = {
                    "target": {
                        "id": result['id'],
                        "table_name_source": result['table_name_source'],
                        "schemas": result['schemas'],
                        "db_source": result['db_source'],
                        "db_target": result['db_target'],
                        "column_date_name": result['column_date_name'],
                        "table_name_target": result['table_name_target'],
                        "data_source_column_name": result['data_source_column_name'],
                        "data_source": result['data_source'],
                        "layer": result['layer'],
                        "flag": result['flag'],
                        "insert_time": result['insert_time'],
                    },
                    "source": []    
                }
                if result['id_2'] is not None: 
                    results_mapped[key]['source'].append({
                        "id": result['id_2'],
                        "table_name_source": result['table_name_source_2'],
                        "schemas": result['schema_2'],
                        "db_source": result['db_source_2'],
                        "db_target": result['db_target_2'],
                        "column_date_name": result['column_date_name_2'],
                        "table_name_target": result['table_name_target_2'],
                        "data_source_column_name": result['data_source_column_name_2'],
                        "data_source": result['data_source_2'],
                        "layer": result['layer_2'],
                        "flag": result['flag_2'],
                        "insert_time": result['insert_time_2'],
                    })
        print(results_mapped)
        return results_mapped
        
    
    def get_param_detail_mapping(self, name: str, flag: str, layer: str): 
        conn = self.__pg_obj.client_connect()
        filters = "1=1" if name is None else f"table_name_source LIKE '%%{name}%%'"
        flag_logic = "select ds.*, dt.* from data_source ds left join data_target dt on ds.table_name_target = dt.table_name_target" if flag == 'source' else f"select dt.*, ds.* from data_target dt left join data_source  ds on dt.table_name_target = ds.table_name_target"
        layer_filters = "1=1" if layer is None else f"layer = '{layer}'"

        if flag == "source": 
            query_datas = f"""
                with data_target as (
                    select * from etl_monitoring.count_mapping cm  
                    where flag = 'target'
                ), data_source as (
                    select * from etl_monitoring.count_mapping cm  
                    where flag = 'source' and {filters} and {layer_filters}
                )
            """
        else: 
            query_datas = f"""
                with data_target as (
                    select * from etl_monitoring.count_mapping cm  
                    where flag = 'target' and {filters} and {layer_filters}
                ), data_source as (
                    select * from etl_monitoring.count_mapping cm  
                    where flag = 'source'
                )
            """

        df = pd.read_sql(
            f"""
                {query_datas}
                {flag_logic}
                order by ds.insert_time desc, dt.insert_time desc;
            """,
            con=conn
        )
        cols = df.columns.to_series()
        series = cols.groupby(cols).cumcount() 
        new_columns = [f"{c}_{i+1}" if i > 0 and c == name else c for c, i, name in zip(cols, series, cols)]
        df.columns = new_columns
        print(df)

        df.rename(columns={'schema': 'schemas'}, inplace=True)
        df['insert_time'] = df['insert_time'].astype(str)
        df['insert_time_2'] = df['insert_time_2'].astype(str)
        df.replace({np.NaN: None}, inplace=True)
        conn.connection.close()

        return self.__mapping_results(df.to_dict('records'), flag)
    
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
        data.update(**payload)

        conn = self.__pg_obj.client_connect_psycopg()
        cursor = conn.cursor()

        cursor.execute(
            f"""
                UPDATE 
                    etl_monitoring.count_mapping
                SET 
                    table_name_source = %s,
                    schema = %s,
                    db_source = %s,
                    db_target = %s,
                    column_date_name = %s,
                    table_name_target = %s,
                    data_source_column_name = %s,
                    data_source = %s,
                    layer = %s,
                    flag = %s
                WHERE 
                    table_name_source = '{table_name}'
                    AND flag = '{flag}'
                    AND layer = '{layer}'
            """,
            (
                data['table_name_source'],
                data['schemas'],
                data['db_source'],
                data['db_target'],
                data['column_date_name'],
                data['table_name_target'],
                data['data_source_column_name'],
                data['data_source'],
                data['layer'],
                data['flag']
            )
        )
        conn.commit()
        row_updated = cursor.rowcount

        cursor.close()
        conn.close()

        return data, row_updated
    
    def delete_param_mapping(self, id: int): 
        conn = self.__pg_obj.client_connect_psycopg()
        cursor = conn.cursor()

        cursor.execute(f"DELETE FROM etl_monitoring.count_mapping WHERE id = {id}")
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
