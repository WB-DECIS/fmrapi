def get_ref_area_codelist(api_params: dict) -> dict: 
    """Function to get an updated version of REF_AREA codelist from the Fusion
    Metadata Registry (FMR).
    Args:
        api_params (dict): Parameters of the API including the endpoint for REF_AREA codelist
    Returns:
        JSON file with the latest version of REF_AREA codelist. This is the structure of the output:
        {
            'Not classified': 'INX',
            'High income': 'HIC',
            'Lower middle income': 'LMC',
            ...
        }
    """
    logger.info('>>>>> Getting values for the codelist %s...', 
                api_params['codelist']['endpoint']['ref_area'])
    # Requesting codelist from API
    try:
        codelist_dict = requests.get(
            api_params['url'] 
            + api_params['endpoint_path'] 
            + api_params['agency'] 
            + api_params['codelist']['endpoint']['ref_area'] + '/' 
            + api_params['version']
            + api_params['format']
            ).json()
    except json.JSONDecodeError:
        print('FMR instance not accessible!')
        exit()

    # Creating a dict out of the codelist for future use
    ref_area_dict = {}
    for economy in codelist_dict['Codelist'][0]['items']:
        ref_area_dict[economy['names'][0]['value']] = economy['id']

    return ref_area_dict

def fmr_auth(
        api_params: dict,
        user: str,
        password: str
        ):
    """Function to authenticate on FMR through the API
    Args:
        api_params (dict): Parameters of the API.
        user (str): Username to authenticate in FMR
        user (str): Password to authenticate in FMR
    Returns:
        Dictionary with the headers needed in each request to the API
    """
    # Generate basic authentication token
    bytes_auth = bytes(f"{user}:{password}", 'utf-8')
    token = 'Basic ' + b64encode(bytes_auth).decode('ascii')

    headers = {
        'Authorization': token, 
        'Content-Type' : 'application/json'
    }

    return headers

def add_single_item_to_codelist(
        api_params: dict,
        codelist_id: str, 
        item_id: str, 
        item_description: str,
        agency: str = 'WB',
        action: str = 'MERGE'
        ):
    """Function to update an item to any codelist
    Args:
        api_params (dict): Parameters of the API.
        codelist_id (str): Codelist ID from FMR.
        item_id (str): ID of the item that is going to be added.
        item_description (str): Description of the item that is going to be added.
        agency (str): Agency of your codelist (defaults to WB)
        action (str): Use `MERGE` for new codes to be added. `REPLACE` for playload with new and existing codes
    Returns:
        Success or Error message
    """
    # Creating the payload
    temp_payload =  {
        "id" : item_id, 
        "names" : [
            {
                "locale" : "en",
                "value" : item_description
            }
        ]
    }

    headers = fmr_auth(api_params, 
                       credentials['fmr']['user'],
                       credentials['fmr']['password'])

    try:
        # Get values from codelist
        req = requests.get(
            api_params['url'] 
            + api_params['endpoint_path'] 
            + agency + '/' 
            + codelist_id
            + api_params['format']
            ).json()

        existing_list = [i['id'] for i in req['Codelist'][0]['items']]
        
        # If the new value does not exists then add it
        if temp_payload['id'] not in existing_list:

            req['Codelist'][0]['items'] = [temp_payload]

            headers['ACTION'] = action

            response = requests.post(
                api_params['url'] 
                + api_params['items']['add_item_endpoint'],
                headers=headers, 
                json = req
            )

            if 'Success' in response.text:
                # print("Added codes(s) successfully!")
                logger.info(">>>>>>>>> Added codes(s) successfully!")
            else:
                # print(response.json())
                logger.info('>>>>>>>>> %s', response.json())

        else:
            # print("ID already exists in codelist")
            logger.info('>>>>>>>>> ID already exists in codelist...')
    
    except json.JSONDecodeError:
        req = requests.get(
            api_params['url'] 
            + api_params['endpoint_path'] 
            + agency + '/' 
            + codelist_id
            + api_params['format']
            ).json()

        if "Blocked" in req.text:
            # print("WebPage Blocked! Restart FMR instance.")
            logger.info('>>>>>>>>> WebPage Blocked! Restart FMR instance.')
        else:
            # print(f"Error connecting API: {req.text}")
            logger.info('>>>>>>>>> Error connecting API: %s', req.text)

    return True

def add_items_to_codelist(
        api_params: dict,
        codelist_id: str, 
        item_list: list,
        agency: str = 'WB',
        action: str = 'MERGE'
        ):
    """Function to add multiple items to any codelist
    Args:
        api_params (dict): Parameters of the API.
        codelist_id (str): Codelist ID from FMR.
        item_id (str): ID of the item that is going to be added.
        item_description (str): Description of the item that is going to be added.
        agency (str): Agency of your codelist (defaults to WB)
        action (str): Use `MERGE` for new codes to be added. `REPLACE` for playload with new and existing codes
    Returns:
        Success or Error message
    """
    logger.info(">>>>> Adding items to codelist %s...", codelist_id)
    # Loop through all elements in the item_list
    for item in item_list:
        # print(item['id'])
        logger.info('>>>>>>> Item: %s', item['id'])
        # Adding items to codelist
        add_single_item_to_codelist(
            api_params, 
            codelist_id, 
            item['id'],
            item['description'],
            agency,
            action
        )
    return True
    
def validate_single_dataset_fmr(
        api_params: dict,
        filepath, 
        delimiter: str = 'comma'
        ) -> list:
    """Function to validate that a CSV file is SDMX compliant. 
    Make sure the file has `STRUCTURE`, `STRUCTURE_ID` and `ACTION` columns
    See this page for more details:
    https://github.com/sdmx-twg/sdmx-csv/blob/master/data-message/docs/sdmx-csv-field-guide.md
    Args:
        api_params (dict): Parameters of the API.
        filepath (str): Full path of the file that is going to be validated
        delimiter (str): delimiter of the CSV file. By default it is 'comma'.
    Returns:
        List with two elements. First element is a bool that indicates True if 
        the dataset was validated successfully and False otherwise. The second
        element is an empty dictionary if the dataset was validated sucessfully
        (no errors are found), or a dictionary with errors related to the 
        validation process against the DSD.
    """

    headers = fmr_auth(api_params, credentials['fmr']['user'],
                    credentials['fmr']['password'])

    val_req = requests.post(
        api_params['url'] + api_params['validation']['load_endpoint'],
        files = {
            "uploadFile" : open(filepath, 'rb')
            },
        headers = {'Data-Format': f'csv;delimiter={delimiter}'}
    )
    
    time.sleep(5)
    
    if val_req.status_code == 200:
        try:
            val_req = val_req.json()

            # load_status = requests.get(f"{url}/ws/public/data/loadStatus", params={"uid" : val_req['uid']}).json()
            load_status = requests.get(
                api_params['url'] + api_params['validation']['load_status_endpoint'],
                params={"uid" : val_req['uid']}
            ).json()

            if 'Errors' in load_status.keys():

                #while load_status['Status'] != "Complete":
                #    sleep(10)

                #    load_status = requests.get(f"{url}/ws/public/data/loadStatus", params={"uid" : val_req['uid']}).json()

                if load_status['Errors']:

                    # print("Validation finished with Errors! JSON report will be exported to working repository")
                    logger.info('>>>>>>>>> Validation finished with Errors! JSON report will be exported to working repository')

                    errors = []

                    for dx in load_status['Datasets']:
                        errors.append(dx['ValidationReport'][0]['Errors'])

                    validated_bool = False
                    error_dict = {"ValidationReport" : errors}
                    
                    # with open(os.path.join(report_path, f"{os.path.basename(filename).split('.')[0]}_ValidationReport.json"), 'w') as f:
                    #     json.dump(error_dict, f, indent=4)

                else:
                    # print(f"Dataset validation complete without Errors!")
                    logger.info('>>>>>>>>> Dataset validation complete without Errors!')
                    validated_bool = True
                    error_dict = {}

            elif 'Error' in load_status.keys():
                # print(f"Session timed out : {load_status['Error']}")
                logger.info('>>>>>>>>> Session timed out : %s', load_status['Error'])
                validated_bool = False
                error_dict = {"ValidationReport" : "Session timed out"}

        except json.JSONDecodeError: 
            # print("Cannot connect to FMR instance!!")
            logger.info('>>>>>>>>> Cannot connect to FMR instance!!')
            validated_bool = False
            error_dict = {"ValidationReport" : "Cannot connect to FMR instance"}

    else:
        # print(f"Error with load endpoint: {val_req.text}")
        logger.info('>>>>>>>>> Error with load endpoint: %s', val_req.text)
        validated_bool = False
        error_dict = {"ValidationReport" : "Error with load endpoint"}

    return validated_bool, error_dict

def validate_datasets_fmr(
        api_params: dict,
        datasets: dict, 
        folder_name: str,
        boolean: bool,
        delimiter: str = 'comma',
        ):
    """Function to validate multiple CSV files is SDMX compliant. 
    Make sure the file has `STRUCTURE`, `STRUCTURE_ID` and `ACTION` columns
    See this page for more details:
    https://github.com/sdmx-twg/sdmx-csv/blob/master/data-message/docs/sdmx-csv-field-guide.md
    Args:
        api_params (dict): Parameters of the API.
        filepath (str): Full path of the file that is going to be validated
        folder_name (str): Name of the folder where the CSV files are saved
        boolean (bool): This is just a flag to force kedro to run after another node
        delimiter (str): delimiter of the CSV file. By default it is 'comma'.
    Returns:
        List of two dictionaries. The first dictionary returns True or False for 
        file, and the second dictionary has errors for each file.
    """
    if boolean:
        logger.info('>>>>> Validating files against DSD...')
        # Define path where the files are located
        # This path is for the python file
        export_path = os.path.join(os.path.abspath(os.getcwd()), 
                                'data', '03_primary', folder_name)
        # This path is for the jupyter notebook
        # export_path = os.path.join(os.path.dirname(os.path.dirname(os.getcwd())), 
        #                         'data', '03_primary', folder_name)

        validated = {}
        error = {}
        for filename in datasets.keys():
            # print(os.path.join(export_path, filename))
            # print(filename)
            logger.info('>>>>>>> File: %s', filename)
            temp_path = os.path.join(export_path, filename)
            temp_validated, temp_error = validate_single_dataset_fmr(api_params, 
                                                    temp_path, 
                                                    delimiter)
            validated[filename] = temp_validated
            error[filename] = temp_error

        return validated, error
    else:
        return None