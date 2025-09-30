tableau_refreshes.py
from datetime import datetime, timezone, timedelta, time
import tableauserverclient as TSC
import pandas as pd
import os
from utils import get_session, get_secret, send_slack_notification, get_webhook


def main():

    creds = {'db': 'wbx_data',
            'cluster_id': 'wbx-data',
            'region': 'us-west-2'}

    # Initializing Botocore client
    session = get_session(creds)

    tableau_secret = get_secret(os.environ['PAT_VALUE'], session, creds['region'])
    webhook_url = get_webhook(session, creds)

    send_slack_notification(webhook_url, 'Starting Tableau tests :loading_dots:')

    PAT_NAME = tableau_secret['PAT_NAME']
    PAT_VALUE = tableau_secret['PAT_VALUE'] # replace after 1 year
    SITE_NAME = 'webconnex'
    SERVER = 'https://us-west-2b.online.tableau.com/'
    
    tableau_auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_VALUE, site_id=SITE_NAME)
    server = TSC.Server(SERVER, use_server_version=True)
    
    datasources = []
    permissions_dict = {}
    tasks = []
    projects = ['Data-team', 'Financial Dashboards', 'Marketing', 'Purchase Protect']
    permissions = {
                'Cohort Account Retention New in Financial Dashboards':  ['Account Manager Admin', 'Data Engineering', 'Internal', 'Data Team Contractors'],
                'PP Orders New in Purchase Protect':  ['Data Engineering', 'Internal'],
                'Cohort Signup Account Revenue New in Marketing':  ['Data Engineering', 'Internal'],
                'Cumulative Customer Waterfall in Marketing':  ['Data Engineering', 'Internal'],
                'Digital Marketing Spend Daily New in Marketing':  ['Data Engineering', 'Internal'],
                'Attach Rate in Purchase Protect':  ['Data Engineering', 'Internal'],
                'Outside Sales Deals 4 Week Snapshot in Financial Dashboards':  ['Data Engineering', 'Internal', 'Account Managers', 'Data Team Contractors'],
                'PP Claims in Purchase Protect':  ['Data Engineering', 'Purchase Protection Claims', 'Internal'],
                'PP Orders and Claims in Purchase Protect':  ['Data Engineering', 'Internal', 'Data Team Contractors'],
                'Upcoming Invoices in Data-team':  ['Data Engineering', 'Internal'],
                'Invoice Declines in Data-team':  ['Data Engineering', 'Internal'],
                'Invoices in Data-team':  ['Data Engineering', 'Internal'],
                'PP Enabled Forms in Purchase Protect':  ['Data Engineering', 'Internal'],
                'Cohort Signup Account Revenue New in Financial Dashboards':  ['Account Manager Admin', 'Data Engineering', 'Internal', 'Data Team Contractors'],
                'Outside Sales Revenue since 2022 in Financial Dashboards':  ['Data Engineering', 'Internal', 'Account Managers', 'Data Team Contractors'],
                'Adyen Transitioning Customers in Data-team':  ['Data Engineering', 'Internal', 'Payments', 'Data Team Contractors'],
                'Adyen Projections in Data-team':  ['Data Engineering', 'Internal', 'Data Team Contractors'],
                'Adyen Current Customers in Data-team':  ['Data Engineering', 'Internal', 'Data Team Contractors'],
                'Event Cohort PP Revenue in Purchase Protect':  ['Data Engineering', 'Internal'],
                'PP Orders New in Data-team':  ['Data Engineering'],
                'Adyen Transition Candidates in Data-team':  ['Data Engineering', 'Internal', 'Data Team Contractors'],
                'Adyen Accounts in Data-team':  ['Data Engineering', 'Internal', 'Data Team Contractors'],
                'Account Conversions in Marketing':  ['Data Engineering', 'Internal', 'Marketing'],
                'Processing Volume in Financial Dashboards':  ['Account Manager Admin', 'Data Engineering', 'Internal', 'Payments']
                }

    with server.auth.sign_in(tableau_auth):
        now = datetime.now(timezone.utc) - timedelta(hours=7)
        
        datasources_data = [ds for ds in TSC.Pager(server.datasources) if ds.has_extracts]
        tasks_data, _ = server.tasks.get()
        
        for ds in datasources_data:
            if ds.project_name in projects:
                datasources.append([(now - (ds.updated_at or now)).days, ds.name, ds.updated_at, ds.project_name])
                wb = ds.name + ' in ' + ds.project_name
                permissions_dict[wb] = []
                server.datasources.populate_permissions(ds)
                ds_permissions = ds.permissions
                
                for rule in ds_permissions:
                    group_user_type = rule.grantee.tag_name
                    group_user_id = rule.grantee.id
                    if group_user_type == 'group':
                        for group_item in TSC.Pager(server.groups):
                            if group_item.id == group_user_id:
                                group_user_name = group_item.name
                                permissions_dict[wb].append(group_user_name)
                                break

        for task in tasks_data:
            datasource = server.datasources.get_by_id(task.target.id)
            interval = task.schedule_item.interval_item
            project_name = datasource.project_name

            if project_name in projects:
                task_info = {
                    'Datasource Name': datasource.name,
                    'Project Name': project_name,
                    'Refreshed Daily': True if isinstance(interval, TSC.DailyInterval) else False,
                    'Start Time': interval.start_time,
                    'In Range': time(2, 0) <= interval.start_time <= time(3, 0)
                }

                tasks.append(task_info)

    # Get the extracts that have not been refreshed
    df_datasources = pd.DataFrame(datasources)
    df_datasources.columns = ['days_since_refresh', 'extract', 'updated_at', 'project']
    df_datasources['updated_at'] = df_datasources['updated_at'].dt.date
    not_refreshed = df_datasources[df_datasources['days_since_refresh'] > 0].sort_values(by = 'days_since_refresh', ascending = False).reset_index(drop = True)
    print('Extracts not refreshed: ', len(not_refreshed))

    if len(not_refreshed) > 0:
        notification_message = ":warning: Tableau extracts last refresh:\n\n"
        for x in not_refreshed[['extract', 'project', 'updated_at']].values:
            notification_message += f"- {x[2]}    :    {x[0]} in {x[1]}\n"

        send_slack_notification(webhook_url, notification_message)

    send_slack_notification(webhook_url, 'Extract freshness test finished :white_check_mark:')
    # ------------------------------------------------------------------------------------

    # Get the missing permissions for each extract
    missing_permissions = []

    for k in permissions.keys():
        try:
            diff = set(permissions[k]).difference(set(permissions_dict[k]))
            if diff:
                missing_permissions.append(k + '    :    ' + ', '.join(diff))
        except:
            send_slack_notification(webhook_url, f":red-x-mark: {k} doesn't exist.")
    
    print('Extracts missing permissions: ', len(missing_permissions))

    if len(missing_permissions) > 0:
        notification_message = ":warning: Tableau extracts missing permissions:\n\n"
        for x in missing_permissions:
            notification_message += f"- {x}\n"

        send_slack_notification(webhook_url, notification_message)

    send_slack_notification(webhook_url, 'Extract permissions test finished :white_check_mark:')
    # ------------------------------------------------------------------------------------
    
    # Get task schedules outside of 2-3 am window
    df_tasks = pd.DataFrame(tasks)
    df_tasks.sort_values(by='Datasource Name', inplace=True, key=lambda col: col.str.lower())
    wrong_schedules = df_tasks[df_tasks['In Range'] == False]

    if len(wrong_schedules) > 0:
        notification_message = ":warning: Please schedule tasks between 2 and 3 am:\n\n"
        for x in wrong_schedules.values:
            notification_message += f"- {x[0]} in {x[1]} (currently at {x[3]})\n"

        send_slack_notification(webhook_url, notification_message)

    send_slack_notification(webhook_url, 'Tasks schedules test finished :white_check_mark:')
            
if __name__ == '__main__':
    main()
