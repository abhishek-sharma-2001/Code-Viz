import hashlib
import hmac
import json
import os

import aiohttp
from src.constants import PME_PARTNER_ID_LIST
from src.constants import PME_DESIGNATION_LIST
from src.db_utils.ga import GAReport
from src.db_utils.mongo import MongoDBConnector
from src.db_utils.sql import SQLConnector
from datetime import datetime, timedelta
from src.exceptions import ServiceDownException
from src.log_utils import logger
from async_retrying import retry
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

class PMENotificationData:
    def __init__(self):
        self.core_db = SQLConnector()
        self.partner_db = MongoDBConnector()
        self.db_eligible_users_curr_month = None
        self.db_eligible_users_prev_month = None
        self.appointments = None
        self.appointments_v2 = None
        self.ga_data = None
        self.eligible_users = None

    async def calculate_date_range(self, current_date, is_previous_month=False):
        logger.info("Inside calculate_date_range")
        current_month = current_date.month
        current_year = current_date.year

        if is_previous_month:
            previous_month = current_month - 1
            previous_year = current_year - 2
            logger.info(f"previous_month: {previous_month}, previous_year: {previous_year}")
            if previous_month == 0:
                previous_month = 12
                previous_year -= 1
            start_date = datetime(previous_year, previous_month, 1)
            end_date = datetime(current_year, current_month, 1) - timedelta(days=1)
            logger.info(f"start_date: {start_date}, end_date: {end_date}")
        else:
            logger.info(f"current_month: {current_month}, current_year: {current_year}")
            start_date = datetime(current_year, current_month, 1)
            end_date = (
                start_date.replace(month=current_month + 1, day=1)
                if current_month < 12
                else start_date.replace(year=current_year + 1, month=1, day=1)
            )
            logger.info(f"start_date: {start_date}, end_date: {end_date}")
        return start_date, end_date

    async def get_aggregate_result(self, col, start_date, end_date, email_filter=None, spouse_filter=False, employee_filter=False):
        logger.info("Inside get_aggregate_result")
        logger.info(f"start_date for aggregate result -> start_date : {start_date}, end_date: {end_date}")
        match_condition = {
            "partner_id": {"$in": PME_PARTNER_ID_LIST},
            "meta_fields.employee_designation": {"$nin": PME_DESIGNATION_LIST},
            "meta_fields.pme_eligibility_start_date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lt": end_date.strftime("%Y-%m-%d"),
            },
        }
        # if employee_filter:
            # match_condition["meta_fields.pme_eligibility_start_date"] = {
            #     "$gte": start_date.strftime("%Y-%m-%d"),
            #     "$lt": end_date.strftime("%Y-%m-%d"),
            # }

        if email_filter:
            match_condition["email"] = {"$in": email_filter}
        if spouse_filter:
            match_condition["family_details"] = {
                "$elemMatch": {
                    "relation": "Spouse",
                    "pme_status": "",
                    # "pme_eligibility_start_date": {
                    #     "$gte": start_date.strftime("%Y-%m-%d"),
                    #     "$lt": end_date.strftime("%Y-%m-%d"),
                    # }
                }
            }

        pipeline = [
            {"$match": match_condition},
            {
                "$lookup": {
                    "from": "partner_user",
                    "let": {
                        "localField1": "$partner_id",
                        "localField2": "$partner_user_id",
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$partner_id", "$$localField1"]},
                                        {
                                            "$eq": [
                                                "$meta_fields.partner_user_id",
                                                "$$localField2",
                                            ]
                                        },
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "joinedData",
                }
            },
            {
                "$project": {
                    "email": 1,
                    "phone": 1,
                    "first_name": "$meta_fields.first_name",
                    "pme_status": "$meta_fields.pme_status",
                    "jhh_user_id": {"$arrayElemAt": ["$joinedData.jhh_user_id", 0]},
                    "gender": "$meta_fields.gender",
                    "family_details": {
                        "$ifNull": [
                            {
                                "$arrayElemAt": [
                                    {
                                        "$filter": {
                                            "input": {"$ifNull": ["$family_details", []]},
                                            "as": "family",
                                            "cond": {
                                                "$and": [
                                                    {"$eq": ["$$family.relation", "Spouse"]},
                                                    {"$eq": ["$$family.pme_status", ""]},
                                                ]
                                            },
                                        }
                                    },
                                    0,
                                ]
                            },
                            {},
                        ]
                    },
                }
            },
        ]
        result = await self.partner_db.execute_aggregate_query(col, pipeline)
        logger.info(f"aggregate result: {result}")
        return result

    async def get_db_eligible_spouse_curr_month(self, employee_email_ids=None):
        logger.info("Inside get_db_eligible_spouse_curr_month")
        current_date = datetime.now()
        start_date, end_date = await self.calculate_date_range(current_date)
        eligible_users = await self.get_aggregate_result(
            "PME_NOTIFICATION_TEST", start_date, end_date, employee_email_ids, spouse_filter=True,
        )
        days_from_start = (current_date - start_date).days
        return eligible_users, days_from_start

    async def get_db_eligible_spouse_prev_month(self, employee_email_ids=None):
        logger.info("Inside get_db_eligible_spouse_prev_month")
        current_date = datetime.now()
        start_date, end_date = await self.calculate_date_range(current_date, is_previous_month=True)
        eligible_users = await self.get_aggregate_result(
            "PME_NOTIFICATION_TEST", start_date, end_date, employee_email_ids, spouse_filter=True
        )
        days_from_start = (current_date - start_date).days
        return eligible_users, days_from_start

    async def get_db_eligible_users_and_spouse_curr_month(self, employee_email_ids=None):
        logger.info("Inside get_db_eligible_users_and_spouse_curr_month")
        current_date = datetime.now()
        start_date, end_date = await self.calculate_date_range(current_date)
        eligible_users = await self.get_aggregate_result(
            "PME_NOTIFICATION_TEST", start_date, end_date, employee_email_ids, spouse_filter=True, employee_filter=True
        )
        days_from_start = (current_date - start_date).days
        return eligible_users, days_from_start

    async def get_db_eligible_users_and_spouse_prev_month(self, employee_email_ids=None):
        logger.info("Inside get_db_eligible_users_and_spouse_prev_month")
        current_date = datetime.now()
        start_date, end_date = await self.calculate_date_range(current_date, is_previous_month=True)
        eligible_users = await self.get_aggregate_result(
            "PME_NOTIFICATION_TEST", start_date, end_date, employee_email_ids, spouse_filter=True, employee_filter=True
        )
        days_from_start = (current_date - start_date).days
        return eligible_users, days_from_start

    async def get_db_eligible_users_curr_month(self, employee_email_ids=None):
        logger.info("Inside get_db_eligible_users_curr_month")
        current_date = datetime.now()
        start_date, end_date = await self.calculate_date_range(current_date)
        eligible_users = await self.get_aggregate_result(
            "PME_NOTIFICATION_TEST", start_date, end_date, employee_email_ids, spouse_filter=False, employee_filter=True
        )
        days_from_start = (current_date - start_date).days
        return eligible_users, days_from_start

    async def get_db_eligible_users_prev_month(self, employee_email_ids=None):
        logger.info("Inside get_db_eligible_users_prev_month")
        current_date = datetime.now()
        start_date, end_date = await self.calculate_date_range(current_date, is_previous_month=True)
        eligible_users = await self.get_aggregate_result(
            "PME_NOTIFICATION_TEST", start_date, end_date, employee_email_ids, spouse_filter=False, employee_filter=True
        )
        days_from_start = (current_date - start_date).days
        return eligible_users, days_from_start

    async def get_appointments_data(self):
        query = """
        SELECT appointments.email, appointments.phone_number, appointments.appointment_datetime, appointments.status, users.healthhub_id, users.name, appointment_details.patient_information FROM appointments
        LEFT JOIN appointment_details on appointment_details.appointment_id=appointments.id
        LEFT JOIN users on appointments.user_id = users.id
        WHERE appointments.appointment_type='100'
        AND appointments.status = '2'
        AND appointments.is_self='1'
        AND appointments.appointment_datetime >= (CURRENT_DATE+INTERVAL '1 day') 
        AND appointments.appointment_datetime <= (CURRENT_DATE + INTERVAL '1 day 23:59:59')
        """
        return await self.core_db.execute_read_query(query)

    async def get_appointments_data_v2(self, days_from_start):
        query = """
        SELECT distinct on (users.healthhub_id) appointments.email, appointments.phone_number, appointments.appointment_datetime, appointments.status, users.healthhub_id, users.name, appointment_details.patient_information FROM appointments
        LEFT JOIN appointment_details on appointment_details.appointment_id=appointments.id
        LEFT JOIN users on appointments.user_id = users.id
        WHERE appointments.appointment_type='100' 
        AND appointments.status IN ('2', '3', '6', '13', '14') 
        AND appointments.appointment_datetime >= (CURRENT_DATE-INTERVAL '{} day')
        AND appointments.is_self='1'
        ORDER BY users.healthhub_id, appointments.appointment_datetime DESC;
        """.format(
            days_from_start
        )
        return await self.core_db.execute_read_query(query)

    async def get_appointments_data_v3(self, days_from_start):
        query = """
        SELECT distinct on (users.healthhub_id) appointments.email, appointments.phone_number, appointments.appointment_datetime, appointments.status, users.healthhub_id, users.name, appointment_details.patient_information FROM appointments
        LEFT JOIN appointment_details on appointment_details.appointment_id=appointments.id
        LEFT JOIN users on appointments.user_id = users.id
        WHERE appointments.appointment_type='100' 
        AND appointments.status IN ('2', '3', '4', '6', '13', '14','15','50') 
        AND appointments.appointment_datetime >= (CURRENT_DATE-INTERVAL '{} day')
        AND (appointments.is_self='1' OR appointments.relation='Spouse')
        ORDER BY users.healthhub_id, appointments.appointment_datetime DESC;
        """.format(
            days_from_start
        )
        return await self.core_db.execute_read_query(query)

    # async def get_ga_data(self, labels):
    #     ga_client = GAReport()
    #     current_date = datetime.now()
    #     start_date, _ = self.calculate_date_range(current_date, is_previous_month=True)
    #     self.ga_data = ga_client.pme_users_run_report_with_filters(
    #         labels, start_date.strftime("%Y-%m-%d")
    #     )
    #     return list(set(self.ga_data))

    # async def get_ril_users_eligible_incomplete(self):
    #     find = {
    #         "partner_id": {"$in": PME_PARTNER_ID_LIST},
    #         "meta_fields.partner_user_id": {
    #             "$in": [
    #                 user.get("partner_user_id")
    #                 for user in await self.get_eligible_users()
    #                 if user.get("meta_fields", {}).get("pme_status") != "Y"
    #             ]
    #         },
    #     }

    #     projection = {"jhh_user_id": 1, "meta_fields": 1}

    #     return await self.partner_db.execute_read_query(
    #         "partner_user", query=find, projection=projection
    #     )

    # async def get_eligible_users(self):
    #     if not self.eligible_users:
    #         self.eligible_users = await self.partner_db.execute_read_query(
    #             "partner_user", query={}, projection={"jhh_user_id": 1, "meta_fields": 1}
    #         )
    #     return self.eligible_users

    async def get_ga_data(self, labels):
        ga_client = GAReport()
        # Get previous month and year
        current_date = datetime.now()
        current_month = current_date.month
        current_year = current_date.year

        previous_month = current_month - 1
        previous_year = current_year

        if previous_month == 0:
            previous_month = 12
            previous_year = current_year - 1

        # Calculate start and end dates for the previous month
        start_date = datetime(previous_year, previous_month, 1)
        self.ga_data = ga_client.pme_users_run_report_with_filters(
            labels, start_date.strftime("%Y-%m-%d")
        )
        return list(set(self.ga_data))

    async def get_ril_users_eligible_incomplete(self):
        find = {
            "partner_id": {"$in": PME_PARTNER_ID_LIST},
            "meta_fields.partner_user_id": {
                "$in": [
                    user.get("partner_user_id")
                    for user in await self.get_eligible_users()
                    if user.get("meta_fields", {}).get("pme_status") != "Y"
                ]
            },
        }

        projection = {"jhh_user_id": 1, "meta_fields": 1}

        return await self.partner_db.execute_read_query(
            "partner_user", query=find, projection=projection
        )

    async def get_eligible_users(self):
        if not self.eligible_users:
            self.eligible_users, _ = await self.get_db_eligible_users_curr_month()
            (
                eligible_users_prev_month,
                _,
            ) = await self.get_db_eligible_users_prev_month()
            self.eligible_users.extend(eligible_users_prev_month)
            return self.eligible_users
        return self.eligible_users


class NotificationAPIUtil:
    def __init__(self):
        self.client_id = os.environ.get("NOTIFICATION_API_CLIENT_ID", "")
        self.checksum_key = os.environ.get("NOTIFICATION_API_CHECKSUM_KEY", "")
        self.base_url = os.environ.get("NOTIFICATION_SERVICE_BASE_URL", "")
        self.ocp_key = os.environ.get("NOTIFICATION_OCP_KEY", "")

    def get_headers(self, body):
        json_payload = json.dumps(body)
        return {
            "client-id": self.client_id,
            "x-request-checksum": hmac.digest(
                self.checksum_key.encode(), json_payload.encode(), hashlib.sha256
            ).hex(),
            "Ocp-Apim-Subscription-Key": self.ocp_key,
        }

    @retry(retry_exceptions=(ServiceDownException,))
    async def trigger_notifications(self, body):
        headers = self.get_headers(body)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url=self.base_url,
                json=body,
                headers=headers,
                ssl=False,
            ) as resp:
                if resp.status in range(200, 300):
                    return
                elif resp.status in range(400, 500):
                    logger.error(
                        f"Request failed with reponse: {await resp.text()} and code: {resp.status}"
                    )
                elif resp.status in range(500, 600):
                    logger.error(
                        f"Request failed with reponse: {await resp.text()} and code: {resp.status} ..retrying"
                    )
                    raise ServiceDownException(await resp.text())


async def get_user_ids_from_appointment_data(appointment_data):
    return set([dict(data).get("healthhub_id") for data in appointment_data])


async def is_appointment_booked_user_id(notification_data, user_id):
    appointment_data = await notification_data.get_appointments_data()
    for data in appointment_data:
        healthhub_id = dict(data).get("healthhub_id")
        if healthhub_id is not None and healthhub_id == user_id:
            return True
    return False


def get_appointment_centre_str(patient_info):
    patient_info_dict = json.loads(patient_info)
    centre = patient_info_dict.get("booking_extra_details", {}).get("center", {})
    location = centre.get("location", {})
    return " ".join(
        [
            centre.get("name"),
            location.get("full_address", ""),
            location.get("city", ""),
            centre.get("state", ""),
            "- " + location.get("pincode", ""),
        ]
    )


def get_user_detail(eligible_users, partner_user_id):
    for user_detail in eligible_users:
        if partner_user_id == user_detail.get("partner_user_id"):
            return user_detail


def get_partner_user_id(eligible_users, user_id):
    for user_detail in eligible_users:
        if user_detail.get("jhh_user_id") == user_id:
            return user_detail.get("meta_fields", {}).get("partner_user_id")