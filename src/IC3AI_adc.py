from advance_data_cleaning import AdvanceDataCleaning
import requests
import pandas as pd

class IC3AI_adc(AdvanceDataCleaning):
    """
    IC3AI advance data cleaning
    """
    def run(self, data, cfg):
        """
        Run IC3AI advance data cleaning
        """
        print("****** IC3AI advance data cleaning")
        # data structure
        #{'full_data': full_data, 'accepted_sessions': accepted_sessions, 'test_method':test_method,'answer_path': answer_path,
        worker_list_report = data['worker_list']
        use_sessions = data['use_sessions']
        
        # step1 : remote desktop indicator
        worker_list_report, use_sessions = remote_desktop_indicator(worker_list_report, use_sessions)
        print('#############################')
        # step2 : eval country to worker list
        worker_list_report, use_sessions = eval_country_to_worker_list(worker_list_report, use_sessions)
        # step3 : worker from multiple countries
        worker_list_report, use_sessions = worker_from_multiple_countries(worker_list_report, use_sessions)

        return worker_list_report, use_sessions


def remote_desktop_indicator(worker_list_report, use_sessions):
    """
    Remote desktop indicator ####### NOT WORKING YET ###### 
    """
    print("****** Remote desktop indicator")
    valid_sessions =[]
    failed_sessions =[]
    failes_assignemnt_ids = []

    for session in use_sessions:
        if session['answer.rdp_exist']=='0':
            valid_sessions.append(session)
        else:            
            failed_sessions.append(session)
            failes_assignemnt_ids.append(session['assignmentid'])

    # TODO update worker list report
    # print the size of each
    print("Valid sessions: ", len(valid_sessions))
    print("Failed sessions: ", len(failed_sessions))
    # go through all worker_list_report
    worker_list_report_update = []
    for wlr in worker_list_report:
        if wlr['assignment'] in failes_assignemnt_ids:
            wlr['remote_desktop_failed'] = '1'
            wlr['accept_and_use'] = 0
            if 'failures' in wlr:
                wlr['failures'].append('remote_desktop')
            else:
                wlr['failures'] = ['remote_desktop']
        else:
            wlr['remote_desktop_failed'] = '0'
        worker_list_report_update.append(wlr)

    return worker_list_report_update, valid_sessions

def get_ip_country(ip):
    try:
        ip = ip.split('/')[0]
        print(ip)
        api_key = 'l1JS7IRpLErM6779dbpHgg7WkkwnizsBL-R8LaIRIK0'
        url = f'https://atlas.microsoft.com/geolocation/ip/json?api-version=1.0&subscription-key={api_key}&api-version=1.0&ip={ip}'

        
        response = requests.get(url)
        data = response.json()    
        # get country from response
        country = data['countryRegion']['isoCode']
        print(country)
        return country.lower()
    except Exception as e:
        print("Error getting country from ip: ", e)
        country = 'unknown'
    


def eval_country_to_worker_list(worker_list_report, use_sessions ):
    """
    Add country to worker list report
    """    
    print("****** Eval country to worker list report")
    accepted_countries = ['us']
    ip_2_country_list = {}
    failes_assignemnt_ids = []
    valid_sessions =[]
    for session  in use_sessions:
        ip = session['x-real-ip']
        # check if ip is already in the list
        if ip in ip_2_country_list.keys():
            country = ip_2_country_list[ip]
        else:
            country = get_ip_country(ip).lower()
            ip_2_country_list[ip] = country
        session['country'] = country
        if country not in accepted_countries:
            failes_assignemnt_ids.append(session['assignmentid'])
        else:
            valid_sessions.append(session)

    print("Failed sessions due to country: ", len(failes_assignemnt_ids))
    # go through all worker_list_report
    worker_list_report_update = []
    # add the country to the worker list report for all workers even those that are already marked as failed to be used in next steps
    for wlr in worker_list_report:
        ip = wlr['ip']
        # check if ip is already in the list
        if ip in ip_2_country_list.keys():
            country = ip_2_country_list[ip]
        else:
            country = get_ip_country(ip).lower()
            ip_2_country_list[ip] = country
        wlr['ip_country'] = country 

        if wlr['assignment'] in failes_assignemnt_ids:
            wlr['ip_country_failed'] = '1'
            wlr['accept_and_use'] = 0
        else:
            wlr['ip_country_failed'] = '0'
        worker_list_report_update.append(wlr)
    
    return worker_list_report_update, valid_sessions

def worker_from_multiple_countries(worker_list_report, use_sessions):
    
    df = pd.DataFrame(worker_list_report)
    # list all workers with more than one country
    workers = df['worker_id'].unique()
    workers_with_multiple_countries = []
    for worker in workers:
        countries = df[df['worker_id']==worker]['ip_country'].unique()
        if len(countries)>1:
            workers_with_multiple_countries.append(worker)
    print("Workers with multiple countries: ", len(workers_with_multiple_countries))
    # reject them
    for wlr in worker_list_report:
        if wlr['worker_id'] in workers_with_multiple_countries:
            wlr['multiple_countries'] = '1'
            wlr['accept_and_use'] = 0
        else:
            wlr['multiple_countries'] = '0'
    
    # reomve them from the use_sessions
    valid_sessions = []
    for session in use_sessions:
        if session['workerid'] not in workers_with_multiple_countries:
            valid_sessions.append(session)
    return worker_list_report, valid_sessions



def get_instance():
    """
    Return IC3AI advance data cleaning
    """
    return IC3AI_adc()