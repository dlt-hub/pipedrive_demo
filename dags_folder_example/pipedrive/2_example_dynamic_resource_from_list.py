"""
Pipedrive api docs: https://developers.pipedrive.com/docs/api/v1

Pipedrive changes or deprecates fields and endpoints without versioning the api.
If something breaks, it's a good idea to check the changelog.
Api changelog: https://developers.pipedrive.com/changelog

To get an api key: https://pipedrive.readme.io/docs/how-to-find-the-api-token
"""

import dlt
import requests


@dlt.source(name="pipedrive")
def pipedrive_source(pipedrive_api_key=dlt.secrets.value):

    endpoints = ['deals','persons', 'stages', 'productFields', 'products', 'pipelines', 'personFields',
                 'users', 'organizations', 'organizationFields', 'activityFields', 'dealFields']

    endpoint_resources = [dlt.resource(_get_endpoint(endpoint, pipedrive_api_key), name=endpoint, write_disposition="replace") for endpoint in endpoints]
    # add activities
    activities_resource = dlt.resource(_get_endpoint('activities', pipedrive_api_key, extra_params={'user_id': 0}), name='activities', write_disposition="replace")
    endpoint_resources.append(activities_resource)
    return endpoint_resources


def _paginated_get(url, headers, params):
    """
    Requests and yields data 500 records at a time
    Documentation: https://pipedrive.readme.io/docs/core-api-concepts-pagination
    """
    # pagination start and page limit
    is_next_page = True
    params['start'] = 0
    params['limit'] = 500
    while is_next_page:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        # yield data only
        data = page['data']
        if data:
            yield data
        # check if next page exists
        pagination_info = page.get("additional_data", {}).get("pagination", {})
        # is_next_page is set to True or False
        is_next_page = pagination_info.get("more_items_in_collection", False)
        if is_next_page:
            params['start'] = pagination_info.get("next_start")


def _get_endpoint(entity, pipedrive_api_key, extra_params=None):
    headers = {"Content-Type": "application/json"}
    params = {'api_token': pipedrive_api_key}
    if extra_params:
        params.update(extra_params)
    url = f'https://app.pipedrive.com/v1/{entity}'
    pages = _paginated_get(url, headers=headers, params=params)
    yield from pages

if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='pipedrive_basic', destination='bigquery', dataset_name='pipedrive_basic')

    #data = list(deals_participants())
    #print(data)
    # run the pipeline with your parameters
    load_info = pipeline.run(pipedrive_source())

    # pretty print the information on data that was loaded
    print(load_info)
