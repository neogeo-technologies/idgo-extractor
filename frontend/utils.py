# -*- coding: utf-8 -*-

from ldap3 import Connection, LEVEL
import os

ROLE_PREFIX = os.environ.get('ROLE_PREFIX', 'ROLE_FONCIER_')
LDAP_URI = os.environ.get('LDAP_URI')
LDAP_BINDDN = os.environ.get('LDAP_BINDDN')
LDAP_PASSWD = os.environ.get('LDAP_PASSWD')
LDAP_ORGS_BASEDN = os.environ.get('LDAP_ORGS_BASEDN')
LDAP_SEARCH_FILTER = os.environ.get('LDAP_SEARCH_FILTER')


def acces_foncier(roles):
    for role in roles:
        if role.startswith(ROLE_PREFIX):
            return True
    return False


def extract_cp(org):
    cnx = Connection(LDAP_URI, LDAP_BINDDN, LDAP_PASSWD, auto_bind=True)

    cnx.search(search_base=LDAP_ORGS_BASEDN,
               search_filter=LDAP_SEARCH_FILTER % org,
               search_scope=LEVEL,
               attributes=["businessCategory", "description"])

    for entry in cnx.entries:
        # check status of org
        if entry['businessCategory'] != 'REGISTERED':
            raise ValueError('Organism is still pending')

        if len(entry['description']) == 0:
            res = []
        else:
            res = ','.join(entry['description']).split(',')
        cnx.unbind()
        return res

    if not cnx.closed:
        cnx.unbind()
    print('Error querying LDAP for org %s: entry does not exist' % org)
    return []
