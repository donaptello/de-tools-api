import pandas as pd

from config.pg_config import JdbcConfig

class PipelineService: 
    def __init__(self):
        self.__pg_obj: JdbcConfig = JdbcConfig()

    def get_status_pipeline(self): 
        conn = self.__pg_obj.client_connect()
        df = pd.read_sql(
            f"""
                with cte_success as (
                    select 
                        count(*) as success_pipeline
                    from etl_monitoring.checking_table_target ctt 
                    where ctt.max_date >= date_today - 1
                ), cte_total_pipeline as (
                    select 
                        count(*) as total_pipeline
                    from etl_monitoring.checking_table_target ctt
                ), cte_result as (
                    select
                        (select * from cte_success) as success_pipeline,
                        (select * from cte_total_pipeline) as total_pipeline
                )select 
                    success_pipeline, 
                    total_pipeline, 
                    (total_pipeline - success_pipeline) as failed_pipeline 
                from cte_result 
            """,
            con=conn
        )
        conn.connection.close()
        return df.to_dict('records')