# -*- coding: utf-8 -*-
from flask import Flask, jsonify
import pandas as pd
import datetime as dt
import numpy as np
import math 
import warnings
import mysql.connector
import seaborn as sns
import matplotlib.pyplot as plt
import time
warnings.filterwarnings('ignore') 

mydb = mysql.connector.connect(
    host="10.11.36.1",
    port="3306", 
    user = 'emjunior',
    passwd = 'sIxg5LAHbH4la3bkwE49',
    database="zabbix"
) 

mycursor = mydb.cursor() 
mycursor.execute("show databases")
myresult = mycursor.fetchall() 
print(mydb) 
print(myresult) 

def trata_e_roda():
    # -*- coding: utf-8 -*-
    pass

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
app.config['JSON_AS_ASCII'] = False
app.secret_key = 'ccm'

trata_e_roda()

@app.route('/home')
def home():
    return 'Olá, CCMers'

@app.route('/hosts')
def hosts():
    dados_hosts = []    
    dados_hosts_final['Hosts'] = dados_hosts
    return jsonify(Data=dados_hosts_final) 

@app.route('/alertas') 
def alertas(): 

    json_final = {
       
    } 

    return jsonify(Data=json_final)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=200)   