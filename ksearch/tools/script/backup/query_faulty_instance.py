#!/usr/bin/python
# -*- coding: UTF-8 -*-

import re
import json
import requests


def query_faulty_instance(hosts):
    '''
        查询状态不正常的store
    '''
    data = {'op_type': 'QUERY_INSTANCE_FLATTEN'}
    url = 'http://' + hosts + '/MetaService/query'
    ks_res = requests.post(url, data=json.dumps(data))
    normal_instance = []
    faulty_instance = []
    if ks_res.status_code == 200:
        for item in ks_res.json()['flatten_instances']:
            if item['status'] == 'NORMAL':
                normal_instance.append(item['address'])
            else:
                faulty_instance.append(item['address'])
    regex = re.compile('ksStore')
    instance_list = []
    for host in faulty_instance:
        ip, _ = host.split(':')
        url = 'http://api.matrix.baidu.com/api/v1/matrix/host/' + ip
        matrix_res = requests.get(url)
        if ks_res.status_code == 200:
            for instance in matrix_res.json()['instances']:
                if re.search(regex, instance):
                    instance_list.append(instance)
                    print
                    instance


if __name__ == '__main__':
    query_faulty_instance('10.152.70.12:8110')
