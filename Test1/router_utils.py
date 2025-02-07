import json
import os
import requests
from dotenv import load_dotenv
from src.models import NotificationPayload, NotificationReceiverDetail
from src.pme_notifications.utils import (
    NotificationAPIUtil,
    PMENotificationData,
    get_appointment_centre_str,
    get_user_ids_from_appointment_data,
)
from src.log_utils import logger
from requests.exceptions import ProxyError, RequestException
load_dotenv()

JHH_API_URL = os.getenv('JHH_API_URL')
JHH_PARTNER_AUTH_ID = os.getenv('JHH_PARTNER_AUTH_ID')
JHH_PARTNER_AUTH_TOKEN = os.getenv('JHH_PARTNER_AUTH_TOKEN')
notification_data = PMENotificationData()
notification_api = NotificationAPIUtil()

fetch_spouse_jhh_id_counter_all = 0
fetch_spouse_jhh_id_counter_success = 0

async def fetch_spouse_jhh_id(phone, gender_code):
    logger.info("Inside fetch_spouse_jhh_id")
    global fetch_spouse_jhh_id_counter_all
    global fetch_spouse_jhh_id_counter_success
    fetch_spouse_jhh_id_counter_all += 1
    url = JHH_API_URL
    headers = {
        'JIOHH-FAMILY-PROFILE-PARTNER-AUTH-ID': JHH_PARTNER_AUTH_ID,
        'JIOHH-FAMILY-PROFILE-PARTNER-AUTH-TOKEN': JHH_PARTNER_AUTH_TOKEN
    }
    data = {
        'country_code': '+91',
        'phone': phone,
        'gender': gender_code
    }
    try:
        logger.info(f"Fetching body: {data}")
        logger.info(f"fetching phone: {phone}")
        logger.info(f"Fetching url: {url}")
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        response_data = response.json()
        logger.info(f"Response data: {response_data}")
        fetch_spouse_jhh_id_counter_success += 1

        # Parse the response to find the spouse and get the jio_id
        for member in response_data.get('contents', {}).get('relationship', []):
            if member.get('family_master_relationship', {}).get('name') == 'Spouse':
                return member.get('jio_id')

    except ProxyError as e:
        logger.error(f"Proxy error occurred: {e}")
    except RequestException as e:
        logger.error(f"Request error occurred: {e}")

    return None

async def add_spouse_data_to_eligible_users(eligible_users):
    logger.info("Inside add_spouse_data_to_eligible_users")
    for user in eligible_users:
        phone = user.get("phone")
        gender = user.get("gender")
        gender_code = "1" if gender == "Male" else "2"

        spouse_jhh_id = await fetch_spouse_jhh_id(phone, gender_code)
        user["spouse_jhh_id"] = spouse_jhh_id

    return eligible_users

async def create_receiver_detail(user_detail, spouse_name):
    logger.info("Inside create_receiver_detail")
    return {
        "user_jhh_id": user_detail.get("jhh_user_id"),
        "employee_name": user_detail.get("first_name"),
        "employee_email": user_detail.get("email"),
        "spouse_name": spouse_name,
    }

async def create_response(data, custom_body, receiver_detail):
    logger.info("Inside create_response")
    return {
        "template_push": "dummy_template_name",
        "custom_subject": data.custom_subject,
        "custom_body": custom_body,
        "notification_type": data.notification_type,
        "partner_id": data.partner_id,
        "common_message": data.common_message,
        "receiver_details": [receiver_detail],
        "action_type": 100,
        "metadata_android": {},
        "metadata_ios": {},
        "payload": {
            "action_type": 100,
            "title": data.custom_subject,
            "message": custom_body,
            "deep_link": "/chat"
        }
    }

async def create_custom_body(spouse_name, message_template):
    logger.info("Inside create_custom_body")
    return message_template.format(spouse_name=spouse_name)

# async def notify_eligible_users(data, eligible_users, user_ids_from_appointments, message_template):
async def notify_eligible_users(data, eligible_users, user_ids_from_appointments, message_template):
    logger.info("Inside notify_eligible_users")
    for user_detail in eligible_users:
        spouse_details = user_detail.get("family_details")
        spouse_name = spouse_details.get("name") if spouse_details else "NA"
        custom_body = await create_custom_body(spouse_name, message_template)
        data.custom_body = custom_body

        user_jhh_id = user_detail.get("jhh_user_id")
        if user_jhh_id is not None:
            receiver_detail = await create_receiver_detail(user_detail, spouse_name)
            response = await create_response(data, custom_body, receiver_detail)
            logger.info(f"Notified eligible user with spouse details: {user_jhh_id}")
            yield response
        else:
            logger.info(f"JHH ID not found for eligible user: {user_detail.get('email')}")

async def pme_eligible_current_month_util_with_spouse_details_util(employee_email_ids):
    logger.info("Inside pme_eligible_current_month_util_with_spouse_details_util")
    data = NotificationPayload()
    data.custom_subject = "Eligible for PME!"
    data.notification_type = "JHH"
    data.partner_id = "JHH"
    data.common_message = True

    if employee_email_ids:
        logger.info(f"Fetching eligible users for provided email IDs: {employee_email_ids}")
        eligible_users, days_from_start = await notification_data.get_db_eligible_spouse_curr_month(employee_email_ids)
    else:
        logger.info("Fetching eligible users for all email IDs (no specific email IDs provided).")
        eligible_users, days_from_start = await notification_data.get_db_eligible_spouse_curr_month()
    logger.info(f"Eligible spouse current month: {eligible_users}")

    eligible_users = await add_spouse_data_to_eligible_users(eligible_users)
    logger.info(f"Eligible users with spouse details added: {eligible_users}")

    global fetch_spouse_jhh_id_counter_all
    global fetch_spouse_jhh_id_counter_success
    logger.info(f"Total fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_all}")
    logger.info(f"Total successful fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_success}")

    logger.info("Fetching appointment data...")
    appointment_data = await notification_data.get_appointments_data_v3(days_from_start)
    logger.info(f"Appointment data: {appointment_data}")

    user_ids_from_appointments = await get_user_ids_from_appointment_data(appointment_data)
    logger.info(f"User IDs from appointments: {user_ids_from_appointments}")

    # Step 1: Filter by 'pme_status' not being 'Y'
    users_without_pme_status = [
        user for user in eligible_users if user.get("pme_status") != "Y"
    ]
    logger.info(f"Users after filtering by 'pme_status != Y': {users_without_pme_status}")

    # Step 2: Exclude users whose 'spouse_jhh_id' is in user_ids_from_appointments
    users_excluding_spouses = [
        user for user in users_without_pme_status 
        if user.get("spouse_jhh_id", "") not in user_ids_from_appointments
    ]
    logger.info(f"Users after excluding those with 'spouse_jhh_id' in appointments: {users_excluding_spouses}")

    # Step 3: Include users whose 'jhh_user_id' is in user_ids_from_appointments
    filtered_users = [
        user for user in users_excluding_spouses 
        if user.get("jhh_user_id", "") in user_ids_from_appointments
    ]
    logger.info(f"Final filtered users: {filtered_users}")

    message_template = "Congratulations! Your spouse {spouse_name} is now eligible for a PME. Click here to book their PME now"
    logger.info("Sending notifications to filtered users...")
    async for response in notify_eligible_users(data, filtered_users, user_ids_from_appointments, message_template):
        yield response

async def pme_eligible_previous_month_util_with_spouse_details_util(employee_email_ids):
    logger.info("Inside pme_eligible_previous_month_util_with_spouse_details_util")
    data = NotificationPayload()
    data.custom_subject = "Eligible for PME!"
    data.notification_type = "JHH"
    data.partner_id = "JHH"
    data.common_message = True

    if employee_email_ids:
        logger.info(f"Fetching eligible users for provided email IDs: {employee_email_ids}")
        eligible_users, days_from_start = await notification_data.get_db_eligible_spouse_prev_month(employee_email_ids)
    else:
        logger.info("Fetching eligible users for all email IDs (no specific email IDs provided).")
        eligible_users, days_from_start = await notification_data.get_db_eligible_spouse_prev_month()
    logger.info(f"Eligible spouse previous month: {eligible_users}")

    eligible_users = await add_spouse_data_to_eligible_users(eligible_users)
    logger.info(f"Eligible users with spouse details added: {eligible_users}")

    global fetch_spouse_jhh_id_counter_all
    global fetch_spouse_jhh_id_counter_success
    logger.info(f"Total fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_all}")
    logger.info(f"Total successful fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_success}")

    logger.info("Fetching appointment data...")
    appointment_data = await notification_data.get_appointments_data_v3(days_from_start)
    logger.info(f"Appointment data: {appointment_data}")

    user_ids_from_appointments = await get_user_ids_from_appointment_data(appointment_data)
    logger.info(f"User IDs from appointments: {user_ids_from_appointments}")

    # Step 1: Filter by 'pme_status' not being 'Y'
    users_without_pme_status = [
        user for user in eligible_users if user.get("pme_status") != "Y"
    ]
    logger.info(f"Users after filtering by 'pme_status != Y': {users_without_pme_status}")

    # Step 2: Exclude users whose 'spouse_jhh_id' is in user_ids_from_appointments
    users_excluding_spouses = [
        user for user in users_without_pme_status 
        if user.get("spouse_jhh_id", "") not in user_ids_from_appointments
    ]
    logger.info(f"Users after excluding those with 'spouse_jhh_id' in appointments: {users_excluding_spouses}")

    # Step 3: Include users whose 'jhh_user_id' is in user_ids_from_appointments
    filtered_users = [
        user for user in users_excluding_spouses 
        if user.get("jhh_user_id", "") in user_ids_from_appointments
    ]
    logger.info(f"Final filtered users: {filtered_users}")

    message_template = "Reminder! Your spouse {spouse_name} is yet to avail their PME. Go ahead and book a PME now for them."
    logger.info("Sending notifications to filtered users...")
    async for response in notify_eligible_users(data, filtered_users, user_ids_from_appointments, message_template):
        yield response

async def pme_eligible_current_month_util_with_users_and_spouse_details_util(employee_email_ids):
    logger.info("Inside pme_eligible_current_month_util_with_users_and_spouse_details_util")
    data = NotificationPayload()
    data.custom_subject = "Eligible for PME!"
    data.notification_type = "JHH"
    data.partner_id = "JHH"
    data.common_message = True

    # Step 1: Fetch eligible users and days from start
    if employee_email_ids:
        logger.info(f"Fetching eligible users and spouses for provided email IDs: {employee_email_ids}")
        eligible_users, days_from_start = await notification_data.get_db_eligible_users_and_spouse_curr_month(employee_email_ids)
    else:
        logger.info("Fetching eligible users and spouses for all email IDs (no specific email IDs provided).")
        eligible_users, days_from_start = await notification_data.get_db_eligible_users_and_spouse_curr_month()
    logger.info(f"Eligible users and spouses for the current month: {eligible_users}")

    # Step 2: Add spouse data to eligible users
    eligible_users = await add_spouse_data_to_eligible_users(eligible_users)
    logger.info(f"Eligible users with added spouse details: {eligible_users}")

    # Log global fetch counters
    global fetch_spouse_jhh_id_counter_all
    global fetch_spouse_jhh_id_counter_success
    logger.info(f"Total fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_all}")
    logger.info(f"Total successful fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_success}")

    # Step 3: Fetch appointment data and extract user IDs
    logger.info("Fetching appointment data...")
    appointment_data = await notification_data.get_appointments_data_v3(days_from_start)
    logger.info(f"Appointment data: {appointment_data}")

    user_ids_from_appointments = await get_user_ids_from_appointment_data(appointment_data)
    logger.info(f"User IDs from appointments: {user_ids_from_appointments}")

    # Step 4: Filter users
    # Sub-step 1: Exclude users with 'pme_status == Y'
    users_without_pme_status = [
        user for user in eligible_users if user.get("pme_status") != "Y"
    ]
    logger.info(f"Users after filtering by 'pme_status != Y': {users_without_pme_status}")

    # Sub-step 2: Exclude users whose 'jhh_user_id' is in user_ids_from_appointments
    users_excluding_self_appointments = [
        user for user in users_without_pme_status 
        if user.get("jhh_user_id", "") not in user_ids_from_appointments
    ]
    logger.info(f"Users after excluding those with 'jhh_user_id' in appointments: {users_excluding_self_appointments}")

    # Sub-step 3: Exclude users whose 'spouse_jhh_id' is in user_ids_from_appointments
    filtered_users = [
        user for user in users_excluding_self_appointments 
        if user.get("spouse_jhh_id", "") not in user_ids_from_appointments
    ]
    logger.info(f"Final filtered users: {filtered_users}")

    # Step 5: Send notifications
    message_template = "Congratulations! You and your spouse {spouse_name} are now eligible for your PME. Click here to book now."
    logger.info("Sending notifications to filtered users...")
    async for response in notify_eligible_users(data, filtered_users, user_ids_from_appointments, message_template):
        yield response

async def pme_eligible_previous_month_util_with_users_and_spouse_details_util(employee_email_ids):
    logger.info("Inside pme_eligible_previous_month_util_with_users_and_spouse_details_util")

    # Step 1: Initialize notification payload
    data = NotificationPayload()
    data.custom_subject = "Eligible for PME!"
    data.notification_type = "JHH"
    data.partner_id = "JHH"
    data.common_message = True

    # Step 2: Fetch eligible users and days from start
    if employee_email_ids:
        logger.info(f"Fetching eligible users and spouses for provided email IDs: {employee_email_ids}")
        eligible_users, days_from_start = await notification_data.get_db_eligible_users_and_spouse_prev_month(employee_email_ids)
    else:
        logger.info("Fetching eligible users and spouses for all email IDs (no specific email IDs provided).")
        eligible_users, days_from_start = await notification_data.get_db_eligible_users_and_spouse_prev_month()
    logger.info(f"Eligible users and spouses for the previous month: {eligible_users}")

    # Step 3: Add spouse data to eligible users
    eligible_users = await add_spouse_data_to_eligible_users(eligible_users)
    logger.info(f"Eligible users with added spouse details: {eligible_users}")

    # Step 4: Log global counters for spouse ID fetches
    global fetch_spouse_jhh_id_counter_all
    global fetch_spouse_jhh_id_counter_success
    logger.info(f"Total fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_all}")
    logger.info(f"Total successful fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_success}")

    # Step 5: Fetch appointment data and extract user IDs
    logger.info("Fetching appointment data...")
    appointment_data = await notification_data.get_appointments_data_v3(days_from_start)
    logger.info(f"Appointment data: {appointment_data}")

    user_ids_from_appointments = await get_user_ids_from_appointment_data(appointment_data)
    logger.info(f"User IDs from appointments: {user_ids_from_appointments}")

    # Step 6: Filter eligible users
    # Sub-step 1: Exclude users with 'pme_status == Y'
    users_without_pme_status = [
        user for user in eligible_users if user.get("pme_status") != "Y"
    ]
    logger.info(f"Users after filtering by 'pme_status != Y': {users_without_pme_status}")

    # Sub-step 2: Exclude users whose 'jhh_user_id' is in user_ids_from_appointments
    users_excluding_self_appointments = [
        user for user in users_without_pme_status 
        if user.get("jhh_user_id", "") not in user_ids_from_appointments
    ]
    logger.info(f"Users after excluding those with 'jhh_user_id' in appointments: {users_excluding_self_appointments}")

    # Sub-step 3: Exclude users whose 'spouse_jhh_id' is in user_ids_from_appointments
    filtered_users = [
        user for user in users_excluding_self_appointments 
        if user.get("spouse_jhh_id", "") not in user_ids_from_appointments
    ]
    logger.info(f"Final filtered users: {filtered_users}")

    # Step 7: Send notifications
    message_template = "Hey! You and your spouse {spouse_name} are yet to avail your PME. Click here and take charge of your health now."
    logger.info("Sending notifications to filtered users...")
    async for response in notify_eligible_users(data, filtered_users, user_ids_from_appointments, message_template):
        yield response

async def pme_eligible_current_month_util(employee_email_ids):
    logger.info("Inside pme_eligible_current_month_util")

    # Step 1: Initialize notification payload
    data = NotificationPayload()
    data.custom_subject = "Eligible for PME!"
    data.custom_body = "Congratulations! You are now eligible for PME. Click here to book your PME now"
    data.notification_type = "JHH"
    data.partner_id = "JHH"
    data.common_message = True
    
    # Step 2: Fetch eligible users and days from start
    if employee_email_ids:
        logger.info(f"Fetching eligible users for provided email IDs: {employee_email_ids}")
        eligible_users, days_from_start = await notification_data.get_db_eligible_users_curr_month(employee_email_ids)
    else:
        logger.info("Fetching eligible users for all email IDs (no specific email IDs provided).")
        eligible_users, days_from_start = await notification_data.get_db_eligible_users_curr_month()
    logger.info(f"Eligible users for the current month: {eligible_users}")
    logger.info(f"Days from start for current month: {days_from_start}")

    # Step 3: Add spouse data to eligible users
    eligible_users = await add_spouse_data_to_eligible_users(eligible_users)
    logger.info(f"Eligible users with added spouse details: {eligible_users}")

    # Step 4: Log global counters for spouse ID fetches
    global fetch_spouse_jhh_id_counter_all
    global fetch_spouse_jhh_id_counter_success
    logger.info(f"Total fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_all}")
    logger.info(f"Total successful fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_success}")

    # Step 5: Fetch appointment data and extract user IDs
    logger.info("Fetching appointment data...")
    appointment_data = await notification_data.get_appointments_data_v3(days_from_start)
    logger.info(f"Appointment data: {appointment_data}")

    user_ids_from_appointments = await get_user_ids_from_appointment_data(appointment_data)
    logger.info(f"User IDs from appointments: {user_ids_from_appointments}")

    # Step 6: Filter eligible users
    # Sub-step 1: Exclude users with 'pme_status == Y'
    users_without_pme_status = [
        user for user in eligible_users if user.get("pme_status") != "Y"
    ]
    logger.info(f"Users after filtering by 'pme_status != Y': {users_without_pme_status}")

    # Sub-step 2: Exclude users whose 'jhh_user_id' is in user_ids_from_appointments
    users_excluding_self_appointments = [
        user for user in users_without_pme_status 
        if user.get("jhh_user_id", "") not in user_ids_from_appointments
    ]
    logger.info(f"Users after excluding those with 'jhh_user_id' in appointments: {users_excluding_self_appointments}")

    # Sub-step 3: Include users with 'spouse_jhh_id' in appointments or without a spouse
    filtered_users = [
        user for user in users_excluding_self_appointments
        if (user.get("spouse_jhh_id") and user.get("spouse_jhh_id") in user_ids_from_appointments)
        or user.get("spouse_jhh_id") is None
    ]
    logger.info(f"Final filtered users: {filtered_users}")

    # Step 7: Send notifications
    message_template = "Congratulations! You are now eligible for PME. Click here to book your PME now."
    logger.info("Sending notifications to filtered users...")
    async for response in notify_eligible_users(data, filtered_users, user_ids_from_appointments, message_template):
        yield response


async def pme_eligible_previous_month_util(employee_email_ids):
    logger.info("Inside pme_eligible_previous_month_util")
    # Step 1: Initialize notification payload
    data = NotificationPayload()
    data.custom_subject = "Eligible for PME!"
    data.custom_body = "Hey! You are yet to avail your PME. Click here and take charge of your health now."
    data.notification_type = "JHH"
    data.partner_id = "JHH"
    data.common_message = True
    
    # Step 2: Fetch eligible users and days from start
    if employee_email_ids:
        logger.info(f"Fetching eligible users for provided email IDs: {employee_email_ids}")
        eligible_users, days_from_start = await notification_data.get_db_eligible_users_prev_month(employee_email_ids)
    else:
        logger.info("Fetching eligible users for all email IDs (no specific email IDs provided).")
        eligible_users, days_from_start = await notification_data.get_db_eligible_users_prev_month()
    logger.info(f"Eligible users for the previous month: {eligible_users}")
    logger.info(f"Days from start for previous month: {days_from_start}")

    # Step 3: Add spouse data to eligible users
    eligible_users = await add_spouse_data_to_eligible_users(eligible_users)
    logger.info(f"Eligible users with added spouse details: {eligible_users}")

    # Step 4: Log global counters for spouse ID fetches
    global fetch_spouse_jhh_id_counter_all
    global fetch_spouse_jhh_id_counter_success
    logger.info(f"Total fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_all}")
    logger.info(f"Total successful fetch spouse JHH ID requests: {fetch_spouse_jhh_id_counter_success}")

    # Step 5: Fetch appointment data and extract user IDs
    logger.info("Fetching appointment data...")
    appointment_data = await notification_data.get_appointments_data_v3(days_from_start)
    logger.info(f"Appointment data: {appointment_data}")

    user_ids_from_appointments = await get_user_ids_from_appointment_data(appointment_data)
    logger.info(f"User IDs from appointments: {user_ids_from_appointments}")

    # Step 6: Filter eligible users
    # Sub-step 1: Exclude users with 'pme_status == Y'
    users_without_pme_status = [
        user for user in eligible_users if user.get("pme_status") != "Y"
    ]
    logger.info(f"Users after filtering by 'pme_status != Y': {users_without_pme_status}")

    # Sub-step 2: Exclude users whose 'jhh_user_id' is in user_ids_from_appointments
    users_excluding_self_appointments = [
        user for user in users_without_pme_status
        if user.get("jhh_user_id", "") not in user_ids_from_appointments
    ]
    logger.info(f"Users after excluding those with 'jhh_user_id' in appointments: {users_excluding_self_appointments}")

    # Sub-step 3: Include users with 'spouse_jhh_id' in appointments or without a spouse
    filtered_users = [
        user for user in users_excluding_self_appointments
        if (user.get("spouse_jhh_id") and user.get("spouse_jhh_id") in user_ids_from_appointments)
        or user.get("spouse_jhh_id") is None
    ]
    logger.info(f"Final filtered users: {filtered_users}")

    # Step 7: Send notifications
    message_template = "Hi there! We have observed that you are yet to avail your PME. Take charge of your health. Click to avail now."
    logger.info("Sending notifications to filtered users...")
    async for response in notify_eligible_users(data, filtered_users, user_ids_from_appointments, message_template):
        yield response

async def pme_completed_util():
    template_variables = []
    receiver_details = []
    data = NotificationPayload()
    data.notification_type = "pme_notification_scheduled"
    data.template_email = data.template_sms = "pme_reminder_give_feedback"
    data.template_variables = template_variables
    data.receiver_details = receiver_details

    for user_detail in await notification_data.get_eligible_users():
        if user_detail.get("meta_fields", {}).get("pme_status") == "Y":
            template_variables.append(
                {
                    "patient_name": user_detail.get("meta_fields", {}).get("first_name"),
                    "pme_feedback_url": os.environ.get("PME_FEEDBACK_URL")
                }
            )
            receiver_details.append(
                NotificationReceiverDetail(
                    **{
                        "mobile": user_detail.get("phone"),
                        "email_id": user_detail.get("email"),
                    }
                )
            )
    logger.info(f"# of notified PME completed users: {len(data.receiver_details)}")
    return json.loads(data.json(exclude_none=True))

async def pme_appointment_booked_util():
    template_variables = []
    receiver_details = []
    data = NotificationPayload()
    data.notification_type = "pme_notification_scheduled"
    data.template_email = "reminder_x_days_before_after_booking"
    data.template_sms = "sms_pme_appointment_reminder"
    data.template_variables = template_variables
    data.receiver_details = receiver_details

    appointment_data = await notification_data.get_appointments_data()
    for user_detail in appointment_data:
        user_detail = dict(user_detail)
        apt_datetime_str = user_detail.get("appointment_datetime")
        formatted_apt_date_time = apt_datetime_str.strftime("%dth %B %Y %I:%M %p")
        template_variables.append(
            {
                "patient_name": user_detail.get("name"),
                "employee_name": user_detail.get("name"),
                "apt_date_time": formatted_apt_date_time,
                "apt_centre": get_appointment_centre_str(user_detail.get("patient_information")),
                "apt_date": formatted_apt_date_time,
                "pme_sms_deeplink": os.environ.get("PME_SMS_APPOINTMENT_BOOKED"),
                "pme_email_deeplink": os.environ.get("PME_EMAIL_APPOINTMENT_BOOKED")
            }
        )
        receiver_details.append(
            NotificationReceiverDetail(
                **{
                    "mobile": user_detail.get("phone_number"),
                    "email_id": user_detail.get("email"),
                }
            )
        )
    logger.info(f"# of notified PME reminder appointment booked users: {len(data.receiver_details)}")
    return json.loads(data.json(exclude_none=True))