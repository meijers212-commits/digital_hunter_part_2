from fastapi import APIRouter
from db.dal import Dal_queris
from db.connections import sql_db_connection
from logger.logger import log_event
from api.api_config import Api_config
from maps_data.DigitalHunter_map import plot_map_with_geometry
from fastapi.responses import FileResponse
import os

config = Api_config(log_event=log_event)

sql = sql_db_connection(
    log_event=log_event,
    port=config.sql_port,
    host=config.sql_host,
    user=config.sql_user,
    password=config.sql_password,
    database=config.sql_database
    )

conn = sql.Get_sql_db_connection()


queris = Dal_queris(
    log_event=log_event,
    connection=conn
    )

router = APIRouter()


@router.get("/Quality_target_movement_alert/")
def Quality_target_movement_alert():

    return queris.Quality_target_movement_alert()


@router.get("/Analysis_of_collection_sources/")
def Analysis_of_collection_sources():

    return queris.Analysis_of_collection_sources()


@router.get("/Finding_new_target/")
def Finding_new_target():

    return queris.Finding_new_target()


@router.get("/Identify_old_goals_that_have_arisen/")
def Identify_old_goals_that_have_arisen():

    return queris.Identify_old_goals_that_have_arisen()


@router.get("/Visualization_of_a_target_trajectory/")
def Visualization_of_a_target_trajectory(entity_id):

    queris.Visualization_of_a_target_trajectory(plot_map_with_geometry=plot_map_with_geometry, entity_id=entity_id)

    file_path = os.path.join("maps_data","graph.png")

    return FileResponse(file_path)
