#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import random
import urllib3
import requests
from requests import exceptions

import logging
import traceback


class AbuProxy(object):
    def __init__(self, user, password, host, port):
        self.rds = None
        proxy_meta = 'http://{0}:{1}'.format(host, port)
        if user is not None:
            proxy_meta = 'http://{0}:{1}@{2}:{3}'.format(user, password, host, port)
        self.proxies = {'http': proxy_meta, 'https': proxy_meta}

    def get_proxy(self):
        return self.proxies


class RequestSession(object):
    def __init__(self, session, headers, proxy, time_out, account, password):
        self.session = session
        self.headers = headers
        self.headers['Connection'] = 'close'
        self.proxies = proxy
        self.time_out = time_out
        self.account = account
        self.password = password
        self.agent = UserAgent()

    def download(self, tasks):
        trans_type = tasks['trans_type']
        url = tasks['url']
        params = tasks.get('params', None)
        data = tasks.get('data', None)
        json = tasks.get('json', None)
        content = ''
        if trans_type == 'get':
            status, response = self.get_url(url, params)
            if 200 == status:
                if len(response.text) > 0:
                    if response.apparent_encoding != 'utf-8':
                        content = response.content.decode(response.apparent_encoding, 'replace')
                    elif response.encoding != 'utf-8':
                        content = response.content.decode('utf-8')
                    else:
                        content = response.text
                else:
                    logging.warning('url[{0}] response [{1}]'.format(url, response.text))
                    return -1, ''
            elif response is not None:
                logging.error('request {0} return status {1}'.format(url, response.status_code))
            return status, content
        elif trans_type == 'post':
            status, response = self.post_url(url, data, params, json)
            if 200 == status:
                if len(response.text) > 0:
                    if response.apparent_encoding != 'utf-8':
                        content = response.content.decode(response.apparent_encoding, 'replace')
                    elif response.encoding != 'utf-8':
                        content = response.content.decode('utf-8')
                    else:
                        content = response.text
                else:
                    logging.warning('url[{0}] response [{1}]'.format(url, response.text))
                    return -1, ''
            elif response is not None:
                logging.error('request {0} return status {1}'.format(url, response.status_code))
            return status, content

    def post_url(self, url, data, params=None, json=None):
        response = None
        timeout_flag = False
        self.headers['User-Agent'] = self.agent.get_agent()
        self.headers['Connection'] = 'close'
        try:
            if self.session is not None:
                response = self.session.post(url, headers=self.headers, data=data, json=json, params=params,
                                             proxies=self.proxies)
            else:
                response = requests.post(url, headers=self.headers, data=data, json=json, params=params,
                                         proxies=self.proxies,
                                         timeout=self.time_out)
        except urllib3.exceptions.ProxyError:
            timeout_flag = True
        except exceptions.ProxyError:
            timeout_flag = True
        except exceptions.ReadTimeout:
            timeout_flag = True
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            return -1, None

        # 针对超时的再次请求一次
        if timeout_flag:
            try:
                if self.session is not None:
                    response = self.session.post(url, headers=self.headers, data=data, json=json, params=params,
                                                 proxies=self.proxies)
                else:
                    response = requests.post(url, headers=self.headers, data=data, json=json, params=params,
                                             proxies=self.proxies,
                                             timeout=self.time_out)
            except urllib3.exceptions.ProxyError:
                return -2, None
            except exceptions.ProxyError:
                return -2, None
            except exceptions.ReadTimeout:
                return -2, None
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                return -1, None
        return response.status_code, response

    def get_url(self, url, params=None):
        response = None
        timeout_flag = False
        self.headers['User-Agent'] = self.agent.get_agent()
        self.headers['Connection'] = 'close'
        try:
            if self.session is not None:
                if self.proxies is not None:
                    response = self.session.get(url, headers=self.headers, params=params, proxies=self.proxies)
                else:
                    response = self.session.get(url, headers=self.headers, params=params)
            else:
                response = requests.get(url, headers=self.headers, params=params, proxies=self.proxies,
                                        timeout=self.time_out)
        except urllib3.exceptions.ProxyError:
            timeout_flag = True
        except exceptions.ProxyError:
            timeout_flag = True
        except exceptions.ReadTimeout:
            timeout_flag = True
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            return -1, None

        # 针对超时的再次请求一次
        if timeout_flag:
            try:
                if self.session is not None:
                    if self.proxies is not None:
                        response = self.session.get(url, headers=self.headers, params=params, proxies=self.proxies)
                    else:
                        response = self.session.get(url, headers=self.headers, params=params)
                else:
                    response = requests.get(url, headers=self.headers, params=params, proxies=self.proxies, timeout=5)
            except urllib3.exceptions.ProxyError:
                return -2, None
            except exceptions.ProxyError:
                return -2, None
            except exceptions.ReadTimeout:
                return -2, None
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                return -1, None
        return response.status_code, response


class UserAgent(object):
    def __init__(self):
        self.agents = [
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.249.0 Safari/532.5',
            'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/532.9 (KHTML, like Gecko) Chrome/5.0.310.0 Safari/532.9',
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.7 (KHTML, like Gecko) Chrome/7.0.514.0 Safari/534.7',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/9.0.601.0 Safari/534.14',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/10.0.601.0 Safari/534.14',
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.20 (KHTML, like Gecko) Chrome/11.0.672.2 Safari/534.20',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.27 (KHTML, like Gecko) Chrome/12.0.712.0 Safari/534.27',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.24 Safari/535.1',
            'Mozilla/5.0 (Windows NT 6.0) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2',
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0 x64; en-US; rv:1.9pre) Gecko/2008072421 Minefield/3.0.2pre',
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-GB; rv:1.9.0.11) Gecko/2009060215 Firefox/3.0.11 (.NET CLR 3.5.30729)',
            'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 GTB5',
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; tr; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 ( .NET CLR 3.5.30729; .NET4.0E)',
            'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
            'Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0a2) Gecko/20110622 Firefox/6.0a2',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:7.0.1) Gecko/20100101 Firefox/7.0.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0b4pre) Gecko/20100815 Minefield/4.0b4pre',
            'Mozilla/5.0 (Windows; U; Windows XP) Gecko MultiZilla/1.6.1.0a',
            'Mozilla/2.02E (Win95; U)',
            'Mozilla/3.01Gold (Win95; I)',
            'Mozilla/4.8 [en] (Windows NT 5.1; U)',
            'Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.4) Gecko Netscape/7.1 (ax)',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:25.0) Gecko/20100101 Firefox/25.0',
        ]

    def get_agent(self):
        return self.agents[random.randint(0, len(self.agents) - 1)]
