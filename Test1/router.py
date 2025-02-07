import traceback
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from src.models import EmployeeRequest
from src.db_utils.mongo import MongoDBConnector
from src.db_utils.sql import SQLConnector
mongo_connector = MongoDBConnector()
core_db = SQLConnector()

from src.pme_notifications.router_utils import (
    pme_appointment_booked_util,
    pme_completed_util,
    pme_eligible_current_month_util,
    pme_eligible_previous_month_util,
    pme_eligible_current_month_util_with_spouse_details_util,
    pme_eligible_previous_month_util_with_spouse_details_util,
    pme_eligible_current_month_util_with_users_and_spouse_details_util,
    pme_eligible_previous_month_util_with_users_and_spouse_details_util,
)
from src.log_utils import logger




pme_router = APIRouter()


@pme_router.post("/current_month_eligible_v2")
async def pme_eligible_current_month_util_with_spouse_details(request: Request):
    status_code = 200
    body = await request.body()
    if body:
        request_data = await request.json()
        # Extract email IDs, defaulting to None if not present
        employee_email_ids = request_data.get("employee_email_ids", None)
    else:
        # If body is empty, set employee_email_ids to None to use the old flow
        employee_email_ids = None
    try:
        # Pass the email IDs to the utility function
        logger.info(f"current month eligible spouse employee_email_ids : {employee_email_ids}")
        responses = []
        async for response in pme_eligible_current_month_util_with_spouse_details_util(employee_email_ids):
            responses.append(response)
        print(f"current month eligible spouse : {responses}")
        return responses
    except Exception as e:
        logger.exception(e)
        return JSONResponse(status_code=500, content=str(e))
    

@pme_router.post("/previous_month_eligible_v2")
async def pme_eligible_previous_month_util_with_spouse_details(request: Request):
    status_code = 200
    body = await request.body()
    if body:
        request_data = await request.json()
        # Extract email IDs, defaulting to None if not present
        employee_email_ids = request_data.get("employee_email_ids", None)
    else:
        # If body is empty, set employee_email_ids to None to use the old flow
        employee_email_ids = None
    try:
        logger.info(f"previous month eligible spouse employee_email_ids : {employee_email_ids}")
        # Pass the email IDs to the utility function
        responses = []
        async for response in pme_eligible_previous_month_util_with_spouse_details_util(employee_email_ids):
            responses.append(response)
        print(f"previous month eligible spouse : {responses}")
        return responses
    except Exception as e:
        status_code = 500
        response = e.args[0]
        logger.exception(e)
    return JSONResponse(status_code=status_code, content=response)

@pme_router.post("/current_month_eligible_v3")
async def pme_eligible_current_month_util_with_users_and_spouse_details(request: Request):
    status_code = 200
    body = await request.body()
    if body:
        request_data = await request.json()
        # Extract email IDs, defaulting to None if not present
        employee_email_ids = request_data.get("employee_email_ids", None)
    else:
        # If body is empty, set employee_email_ids to None to use the old flow
        employee_email_ids = None
    try:
        logger.info(f"current month eligible users and spouse employee_email_ids : {employee_email_ids}")
        # Pass the email IDs to the utility function
        responses = []
        async for response in pme_eligible_current_month_util_with_users_and_spouse_details_util(employee_email_ids):
            responses.append(response)
        print(f"current month eligible users and spouse : {responses}")
        return responses
        # response = await pme_eligible_current_month_util_with_users_and_spouse_details_util()
    except Exception as e:
        status_code = 500
        response = e.args[0]
        logger.exception(e)
    return JSONResponse(status_code=status_code, content=response)

@pme_router.post("/previous_month_eligible_v3")
async def pme_eligible_previous_month_util_with_users_and_spouse_details(request: Request):
    status_code = 200
    # Read the raw body content
    body = await request.body()
    if body:
        request_data = await request.json()
        # Extract email IDs, defaulting to None if not present
        employee_email_ids = request_data.get("employee_email_ids", None)
    else:
        # If body is empty, set employee_email_ids to None to use the old flow
        employee_email_ids = None
    try:
        logger.info(f"previous month eligible users and spouse employee_email_ids : {employee_email_ids}")
        # Pass the email IDs to the utility function
        responses = []
        
        async for response in pme_eligible_previous_month_util_with_users_and_spouse_details_util(employee_email_ids):
            responses.append(response)
        print(f"previous month eligible users and spouse : {responses}")
        return responses
        # response = await pme_eligible_previous_month_util_with_users_and_spouse_details_util()
    except Exception as e:
        status_code = 500
        response = e.args[0]
        logger.exception(e)
    return JSONResponse(status_code=status_code, content=response)

@pme_router.post("/current_month_eligible")
async def pme_eligible_current_month(request: Request):
    status_code = 200
    body = await request.body()
    if body:
        request_data = await request.json()
        # Extract email IDs, defaulting to None if not present
        employee_email_ids = request_data.get("employee_email_ids", None)
    else:
        # If body is empty, set employee_email_ids to None to use the old flow
        employee_email_ids = None
    try:
        logger.info(f"current month eligible employee_email_ids : {employee_email_ids}")
        # Pass the email IDs to the utility function
        responses = []
        async for response in pme_eligible_current_month_util(employee_email_ids):
            responses.append(response)
        print(f"current month eligible : {responses}")
        return responses
        # response = await pme_eligible_current_month_util()
    except Exception as e:
        status_code = 500
        response = e.args[0]
        logger.exception(e)
    return JSONResponse(status_code=status_code, content=response)


@pme_router.post("/previous_month_eligible")
async def pme_eligible_previous_month(request: Request):
    status_code = 200
    body = await request.body()
    if body:
        request_data = await request.json()
        # Extract email IDs, defaulting to None if not present
        employee_email_ids = request_data.get("employee_email_ids", None)
    else:
        # If body is empty, set employee_email_ids to None to use the old flow
        employee_email_ids = None
    try:
        logger.info(f"previous month eligible employee_email_ids : {employee_email_ids}")
        # Pass the email IDs to the utility function
        responses = []
        async for response in pme_eligible_previous_month_util(employee_email_ids):
            responses.append(response)
        print(f"previous month eligible :{responses}")
        return responses
        # response = await pme_eligible_previous_month_util()
    except Exception as e:
        status_code = 500
        response = e.args[0]
        logger.exception(e)
    return JSONResponse(status_code=status_code, content=response)

@pme_router.get("/completed")
async def pme_completed():
    status_code = 200
    try:
        response = await pme_completed_util()
    except Exception as e:
        status_code = 500
        response = e.args[0]
        logger.exception(e)
    return JSONResponse(status_code=status_code, content=response)


@pme_router.get("/appointment_booked")
async def pme_appointment_booked():
    status_code = 200
    try:
        response = await pme_appointment_booked_util()
    except Exception as e:
        status_code = 500
        response = e.args[0]
        logger.exception(e)
    return JSONResponse(status_code=status_code, content=response)


@pme_router.post("/get_employee_data")
async def get_employee_data(employee_request: EmployeeRequest):
    try:
        # logic to retrieve jhh_user_ids and execute the SQL query
        employee_ids = employee_request.employee_ids
        print(employee_ids)
        jhh_user_ids = await mongo_connector.get_jhh_user_ids("partner_user", employee_ids)
        print(jhh_user_ids)
        healthhub_ids = await get_healthhub_ids_list(jhh_user_ids)
        print(f"Processed Healthhub IDs: {healthhub_ids}")
        employee_user_ids = await mongo_connector.get_employee_user_ids("partner_user", healthhub_ids)
        print(employee_user_ids)
        response = {"employee_user_ids": employee_user_ids}
        status_code=200
    except Exception as e:
        status_code = 500
        response = str(e)
        logger.exception(e)
    return JSONResponse(status_code=status_code, content=response)


async def get_healthhub_ids_list(healthhub_ids):
    query = """
    SELECT distinct on (users.healthhub_id) users.healthhub_id, appointments.appointment_datetime FROM appointments
    LEFT JOIN users on appointments.user_id = users.id
    WHERE appointments.appointment_type='100' 
    AND appointments.status IN ('2', '3', '6', '13', '14') 
    AND appointments.is_self='1'
    AND users.healthhub_id in({})
    ORDER BY users.healthhub_id, appointments.appointment_datetime DESC;
    """.format(
        "'"+ "', '".join(healthhub_ids) + "'"
    )
    # Execute the SQL query and obtain the results
    result = await core_db.execute_read_query(query)

    healthhub_ids_list = [dict(data).get("healthhub_id") for data in result]

    # Extract the healthhub_ids from the query results and convert them to a list of strings

    return healthhub_ids_list


