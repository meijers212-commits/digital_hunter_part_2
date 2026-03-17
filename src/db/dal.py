class Dal_queris:

    def __init__(self,log_event=None, connection=None):

        self.log_event = log_event
        self.connection = connection
        self.cursor = self.connection.cursor()

    # 1
    def Quality_target_movement_alert(self) -> dict:
        try:

            query = '''
                    SELECT entity_id, target_name, priority_level, movement_distance_km FROM targets
                    WHERE priority_level in (1,2) AND movement_distance_km > 5
                    '''
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            data = {"result":result}
            return data
        
        except Exception as e:
            self.log_event(level="error",message=f"Error while executing query: {e}")
 
    # 2
    def Analysis_of_collection_sources(self):

        try:

            query = '''
                    SELECT signal_type, COUNT(*) AS singel_count FROM intel_signals 
                    GROUP BY signal_type  ORDER BY singel_count DESC 
                    '''
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            data = {"result":result}
            return data
        
        
        except Exception as e:
            self.log_event(level="error",message=f"Error while executing query: {e}")

    # 3
    def Finding_new_target(self):

        try:
        
            query = '''
                    SELECT entity_id, COUNT(*) AS report_count FROM intel_signals
                    WHERE entity_id LIKE '%unknown%' GROUP BY entity_id ORDER BY report_count DESC LIMIT 3
                    '''
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            data = {"result":result}
            return data
        
        
        except Exception as e:
            self.log_event(level="error",message=f"Error while executing query: {e}")

    # 4
    def Identify_old_goals_that_have_arisen(self):

        try:

            query = '''
                WITH day AS (SELECT DISTINCT entity_id FROM intel_signals
                WHERE (timestamp LIKE '% 08:%' or
                timestamp LIKE '% 09:%' OR
                timestamp LIKE '% 10:%' OR
                timestamp LIKE '% 11:%' OR
                timestamp LIKE '% 12:%' OR
                timestamp LIKE '% 13:%' OR
                timestamp LIKE '% 14:%' OR
                timestamp LIKE '% 15:%' OR
                timestamp LIKE '% 16:%' OR
                timestamp LIKE '% 17:%' OR
                timestamp LIKE '% 18:%' OR
                timestamp LIKE '% 19:%' OR
                timestamp LIKE '% 20:%') 
                AND distance_from_last > 0),

                night AS (SELECT entity_id , sum(distance_from_last) as sum_distance_from_last FROM intel_signals
                WHERE (timestamp LIKE '% 21:%' or
                timestamp LIKE '% 22:%' OR
                timestamp LIKE '% 23:%' OR
                timestamp LIKE '% 24:%' OR
                timestamp LIKE '% 00:%' OR
                timestamp LIKE '% 01:%' OR
                timestamp LIKE '% 02:%' OR
                timestamp LIKE '% 03:%' OR
                timestamp LIKE '% 04:%' OR
                timestamp LIKE '% 05:%' OR
                timestamp LIKE '% 06:%' OR
                timestamp LIKE '% 07:%' OR
                timestamp LIKE '% 08:%')
                GROUP BY entity_id HAVING SUM_distance_from_last >= 10)

                SELECT n.entity_id, n.sum_distance_from_last FROM night n 
                LEFT JOIN day d ON n.entity_id = d.entity_id
                WHERE d.entity_id IS NULL
                    '''
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            data = {"result":result}
            return data
        
        except Exception as e:
            self.log_event(level="error",message=f"Error while executing query: {e}")
    
    # 5
    def Visualization_of_a_target_trajectory(self,plot_map_with_geometry, entity_id):

        try:

            query = f'''
                    SELECT reported_lon,reported_lon FROM intel_signals
                    WHERE entity_id LIKE '{entity_id}'
                    '''
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            plot_map_with_geometry(coords=result)

        
        except Exception as e:
            self.log_event(level="error",message=f"Error while executing query: {e}")        


