"""Utilities for SF Login."""
import requests
import xml.dom.minidom
import pandas as pd
import csv
import json


def getUniqueElementValueFromXmlString(xmlString, elementName):
    """
    Extracts an element value from an XML string.
    For example, invoking
    getUniqueElementValueFromXmlString(
        '<?xml version="1.0" encoding="UTF-8"?><foo>bar</foo>', 'foo')
    should return the value 'bar'.
    """
    xmlStringAsDom = xml.dom.minidom.parseString(xmlString)
    elementsByName = xmlStringAsDom.getElementsByTagName(elementName)
    elementValue = None
    if len(elementsByName) > 0:
        elementValue = elementsByName[0].toxml().replace(
            '<' + elementName + '>', '').replace('</' + elementName + '>', '')
    return elementValue


class Salesforce(object):
    def __init__(self,
                 username=None,
                 password=None,
                 security_token=None,
                 client_id='DataSD_Poseidon',
                 sf_version='29.0',
                 domain='sdgov.my'):

        self.sf_version = sf_version
        self.domain = domain
        self.client_id = client_id
        self.session_id = None

        # Security Token Soap request body
        login_soap_request_body = """<?xml version="1.0" encoding="utf-8" ?>
        <env:Envelope
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:env="http://schemas.xmlsoap.org/soap/envelope/"
                xmlns:urn="urn:partner.soap.sforce.com">
            <env:Header>
                <urn:CallOptions>
                    <urn:client>{client_id}</urn:client>
                    <urn:defaultNamespace>sf</urn:defaultNamespace>
                </urn:CallOptions>
            </env:Header>
            <env:Body>
                <n1:login xmlns:n1="urn:partner.soap.sforce.com">
                    <n1:username>{username}</n1:username>
                    <n1:password>{password}{token}</n1:password>
                </n1:login>
            </env:Body>
        </env:Envelope>""".format(
            username=username,
            password=password,
            token=security_token,
            client_id=client_id)

        soap_url = 'https://{domain}.salesforce.com/services/Soap/u/{sf_version}'

        soap_url = soap_url.format(domain=domain, sf_version=sf_version)

        login_soap_request_headers = {
            'content-type': 'text/xml',
            'charset': 'UTF-8',
            'SOAPAction': 'login'
        }
        response = requests.post(
            soap_url,
            login_soap_request_body,
            headers=login_soap_request_headers)

        if response.status_code != 200:
            raise Exception("Login Failed")

        self.session_id = getUniqueElementValueFromXmlString(response.content,
                                                             'sessionId')
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.session_id,
            'X-PrettyPrint': '1'
        }

    def get_report_df(self, report_id):
        url = "https://{domain}.salesforce.com/{report_id}?view=d&snip&export=1&enc=UTF-8&xf=csv"
        url = url.format(report_id=report_id, domain=self.domain)
        resp = requests.get(url,
                            headers=self.headers,
                            cookies={'sid': self.session_id})

        lines = resp.content.splitlines()
        reader = csv.reader(lines)
        data = list(reader)
        data = data[:-7]
        df = pd.DataFrame(data)
        df.columns = df.iloc[0]
        df = df.drop(0)

        return df


    def get_query_records(self, query_string):
        url = "https://{domain}.salesforce.com/services/data/v{sf_version}/query/?q={query_string}"
        url = url.format(domain=self.domain,
                         sf_version=self.sf_version,
                         query_string=query_string)
        resp = requests.get(url,
                            headers=self.headers,
                            cookies={'sid': self.session_id})
        content = json.loads(resp.content)
        records = content['records']

        return records
