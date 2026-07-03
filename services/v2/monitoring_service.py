import pandas as pd 

from config.pg_config import JdbcConfig

class MonitoringService: 
    def __init__(self):
        self.__pg_obj: JdbcConfig = JdbcConfig()

    def get_list_table(self, search: str) -> list: 
        if search is None: 
            search = "1=1"
        else: 
            search = f"b.table_name like '%%{search}%%' OR c.table_name like '%%{search}%%'"

        query = f"""
            select 
                a.id,
                a.parent_id, 
                b.table_name as parent_table_name,
                b."domain" as parent_domain,
                b.layer as parent_layer,
                b."interval" as parent_interval,
                b.asal_data_tag as parent_asal_data,
                c.table_name as child_table_name,
                c."domain" as child_domain,
                c.layer as child_layer,
                c."interval" as child_interval,
                c.asal_data_tag as child_asal_data,
                a.created_at
            from etl_monitoring.td_rowcount_lineage a
            left join etl_monitoring.td_rowcount_table b on a.parent_id = b.id 
            left join etl_monitoring.td_rowcount_table c on a.child_id = c.id
            where {search}
            order by a.created_at desc;
        """

        conn = self.__pg_obj.client_connect()
        df = pd.read_sql(
            sql=query,
            con=conn
        )
        df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
        conn.connection.close()

        return df.to_dict('records')