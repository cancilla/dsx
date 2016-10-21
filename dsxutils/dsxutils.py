import pkg_resources, pip, requests, io, tarfile, json

def get_package_version(pkg_name):
    return  pkg_resources.get_distribution(pkg_name).version

def upgrade_package(pkg_name):
    pip.main(['install', '--upgrade', pkg_name])

def get_file_content(credentials):
    """For given credentials, this functions returns a BytesIO object containg the file content
    from the associated Bluemix Object Storage V3."""

    url1 = ''.join([credentials['auth_url'], '/v3/auth/tokens'])
    data = {'auth': {'identity': {'methods': ['password'],
            'password': {'user': {'name': credentials['username'],'domain': {'id': credentials['domain_id']},
            'password': credentials['password']}}}}}
    headers1 = {'Content-Type': 'application/json'}
    resp1 = requests.post(url=url1, data=json.dumps(data), headers=headers1)
    resp1_body = resp1.json()
    for e1 in resp1_body['token']['catalog']:
        if(e1['type']=='object-store'):
            for e2 in e1['endpoints']:
                if(e2['interface']=='public'and e2['region']==credentials['region']):
                    url2 = ''.join([e2['url'],'/', credentials['container'], '/', credentials['filename']])
    s_subject_token = resp1.headers['x-subject-token']
    headers2 = {'X-Auth-Token': s_subject_token, 'accept': 'application/json'}
    resp2 = requests.get(url=url2, headers=headers2)

    return io.BytesIO(resp2.content).getvalue()

def write_file_to_disk(credentials):
    content = get_file_content(credentials)
    filename = credentials['filename']
    with open(filename, 'wb') as f:
        f.write(content)
    f.close()

def write_and_extract_tarball(credentials):
    write_file_to_disk(credentials)
    tar = tarfile.open(credentials['filename'])
    tar.extractall()
    tar.close()
