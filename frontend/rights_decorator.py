# -*- coding: utf-8 -*-

from functools import wraps
#from flask import request, redirect, g
#from utils import acces_foncier


def rights_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # TODO: get LDAP org object, check businessCategory: REGISTERED
        #if g.username is None or not acces_foncier(g.roles):
        #    return redirect(request.url + '?login')
        return f(*args, **kwargs)
    return decorated_function
